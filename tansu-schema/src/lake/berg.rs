// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::{BTreeMap, HashMap},
    env::vars,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use crate::{
    AsArrow as _, Error, Registry, Result,
    lake::{
        FinalizeTransactionRequest, LakeHouse, LakeHouseType, LakeWriteRequest,
        StageTransactionalRequest,
    },
};
use async_trait::async_trait;
use iceberg::memory::MemoryCatalogBuilder;
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent,
    io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY},
    spec::{DataFileFormat, Schema, TableMetadataBuilder},
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
            rolling_writer::RollingFileWriterBuilder,
        },
    },
};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use parquet::file::properties::WriterProperties;
use tansu_sans_io::record::inflated::Batch;
use tracing::{debug, error, warn};
use url::Url;
use uuid::Uuid;

use super::House;

fn env_mapping(k: &str) -> Option<&str> {
    match k {
        "AWS_ACCESS_KEY_ID" => Some(S3_ACCESS_KEY_ID),
        "AWS_SECRET_ACCESS_KEY" => Some(S3_SECRET_ACCESS_KEY),
        "AWS_DEFAULT_REGION" => Some(S3_REGION),
        "AWS_ENDPOINT" => Some(S3_ENDPOINT),
        _ => None,
    }
}

pub fn env_s3_props() -> impl Iterator<Item = (String, String)> {
    vars().filter_map(|(k, v)| env_mapping(k.as_str()).map(|k| (k.to_owned(), v)))
}

#[derive(Clone, Debug, Default)]
pub struct Builder<C = PhantomData<Url>, L = PhantomData<Url>, R = PhantomData<Registry>> {
    location: L,
    catalog: C,
    schema_registry: R,
    namespace: Option<String>,
    warehouse: Option<String>,
}

impl<C, L, R> Builder<C, L, R> {
    pub fn location(self, location: Url) -> Builder<C, Url, R> {
        Builder {
            location,
            catalog: self.catalog,
            schema_registry: self.schema_registry,
            namespace: self.namespace,
            warehouse: self.warehouse,
        }
    }

    pub fn catalog(self, catalog: Url) -> Builder<Url, L, R> {
        Builder {
            location: self.location,
            catalog,
            schema_registry: self.schema_registry,
            namespace: self.namespace,
            warehouse: self.warehouse,
        }
    }

    pub fn schema_registry(self, schema_registry: Registry) -> Builder<C, L, Registry> {
        Builder {
            catalog: self.catalog,
            location: self.location,
            schema_registry,
            namespace: self.namespace,
            warehouse: self.warehouse,
        }
    }

    pub fn namespace(self, namespace: Option<String>) -> Self {
        Self { namespace, ..self }
    }

    pub fn warehouse(self, warehouse: Option<String>) -> Self {
        Self { warehouse, ..self }
    }
}

impl Builder<Url, Url, Registry> {
    pub async fn build(self) -> Result<House> {
        Iceberg::new(self).await.map(House::Iceberg)
    }
}

#[derive(Clone, Debug)]
pub struct Iceberg {
    catalog: Arc<dyn Catalog>,
    namespace: String,
    tables: Arc<Mutex<HashMap<String, Table>>>,
    staged_writes: Arc<Mutex<BTreeMap<TxnKey, Vec<PendingWrite>>>>,
    schema_registry: Registry,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct TxnKey {
    transaction_id: String,
    producer_id: i64,
    producer_epoch: i16,
}

impl TxnKey {
    fn new(transaction_id: &str, producer_id: i64, producer_epoch: i16) -> Self {
        Self {
            transaction_id: transaction_id.to_owned(),
            producer_id,
            producer_epoch,
        }
    }
}

#[derive(Clone, Debug)]
struct PendingWrite {
    topic: String,
    partition: i32,
    offset: i64,
    batch: Batch,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct TransactionWrite<'a> {
    pub partition: i32,
    pub offset: i64,
    pub batch: &'a Batch,
}

impl Iceberg {
    async fn new(value: Builder<Url, Url, Registry>) -> Result<Self> {
        let catalog = iceberg_catalog(&value.catalog, value.warehouse.clone()).await?;
        Ok(Self {
            catalog,
            namespace: value.namespace.unwrap_or(String::from("tansu")),
            tables: Arc::new(Mutex::new(HashMap::new())),
            staged_writes: Arc::new(Mutex::new(BTreeMap::new())),
            schema_registry: value.schema_registry,
        })
    }
}

async fn iceberg_catalog(catalog: &Url, warehouse: Option<String>) -> Result<Arc<dyn Catalog>> {
    debug!(%catalog, ?warehouse);

    match (catalog.scheme(), catalog.path()) {
        ("http" | "https", "/") | ("http" | "https", _) => {
            let uri = if catalog.path() == "/" {
                format!(
                    "{}://{}:{}",
                    catalog.scheme(),
                    catalog.host_str().unwrap_or("localhost"),
                    catalog.port().unwrap_or(80)
                )
            } else {
                catalog.to_string()
            };

            let mut props: HashMap<String, String> = env_s3_props().collect();
            _ = props.insert(REST_CATALOG_PROP_URI.to_string(), uri);
            if let Some(wh) = warehouse {
                _ = props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), wh);
            }

            let catalog = RestCatalogBuilder::default()
                .load("rest", props)
                .await
                .map_err(|e| Error::Iceberg(Box::new(e)))?;

            Ok(Arc::new(catalog) as Arc<dyn Catalog>)
        }

        ("memory", _) => {
            let mut props = HashMap::new();
            _ = props.insert(
                REST_CATALOG_PROP_WAREHOUSE.to_string(),
                warehouse.unwrap_or_else(|| String::from("memory://warehouse")),
            );

            let catalog = MemoryCatalogBuilder::default()
                .load("memory", props)
                .await
                .map_err(|e| Error::Iceberg(Box::new(e)))?;
            Ok(Arc::new(catalog) as Arc<dyn Catalog>)
        }

        (_otherwise, _) => Err(Error::UnsupportedIcebergCatalogUrl(catalog.to_owned())),
    }
}

impl Iceberg {
    async fn create_namespace(&self) -> Result<NamespaceIdent> {
        let namespace_ident = NamespaceIdent::new(self.namespace.clone());
        debug!(%namespace_ident);

        if !self
            .catalog
            .namespace_exists(&namespace_ident)
            .await
            .inspect(|namespace| debug!(?namespace))
            .inspect_err(|err| debug!(?err))?
        {
            _ = self
                .catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await
                .inspect(|namespace| debug!(?namespace))
                .inspect_err(|err| debug!(?err))?;
        }

        Ok(namespace_ident)
    }

    async fn load_or_create_table(&self, name: &str, schema: Schema) -> Result<Table> {
        if let Some(table) = self.tables.lock().map(|guard| guard.get(name).cloned())? {
            return Ok(table);
        }

        let namespace_ident = self.create_namespace().await?;
        let table_ident = TableIdent::new(namespace_ident.clone(), name.into());

        let table = if self.catalog.table_exists(&table_ident).await? {
            let table = self
                .catalog
                .load_table(&table_ident)
                .await
                .inspect_err(|err| debug!(?err))?;

            if table.metadata().current_schema().as_ref() != &schema {
                debug!(current = ?table.metadata(), ?schema);

                _ = TableMetadataBuilder::new_from_metadata(
                    table.metadata().to_owned(),
                    table
                        .metadata_location()
                        .map(|location| location.to_owned()),
                )
                .add_schema(schema.clone())?
                .set_current_schema(-1)?
                .build()
                .inspect(|update| {
                    debug!(?update.metadata);
                    debug!(?update.changes);
                    debug!(?update.expired_metadata_logs);
                })?;
            }

            table
        } else {
            self.catalog
                .create_table(
                    &namespace_ident,
                    TableCreation::builder()
                        .name(name.into())
                        .schema(schema.clone())
                        .build(),
                )
                .await
                .inspect(|table| debug!(?table))
                .inspect_err(|err| debug!(?err))?
        };

        _ = self
            .tables
            .lock()
            .map(|mut guard| guard.insert(name.to_owned(), table.clone()))?;

        Ok(table)
    }

    pub(crate) async fn store_transaction(
        &self,
        topic: &str,
        writes: &[TransactionWrite<'_>],
    ) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }

        let first = writes[0];

        let first_record_batch = self
            .schema_registry
            .as_arrow(topic, first.partition, first.batch, LakeHouseType::Iceberg)
            .await?;
        let first_schema = first_record_batch.schema();

        debug!(?first_record_batch);
        debug!(schema = ?first_record_batch.schema());

        let schema = Schema::try_from(first_record_batch.schema().as_ref())
            .inspect(|schema| {
                for field in schema.as_struct().fields() {
                    debug!(?field);
                }
            })
            .inspect_err(|err| debug!(?err))?;

        let table = self
            .load_or_create_table(topic, schema.clone())
            .await
            .inspect(|table| {
                for field in table.metadata().current_schema().as_struct().fields() {
                    debug!(?field);
                }
            })
            .inspect_err(|err| debug!(?err))?;

        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::default(),
            table.metadata().current_schema().clone(),
        );

        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            DefaultLocationGenerator::new(table.metadata().clone())?,
            DefaultFileNameGenerator::new(
                topic.to_owned(),
                Some(format!(
                    "txn-{partition:0>10}-{offset:0>20}",
                    partition = first.partition,
                    offset = first.offset
                )),
                DataFileFormat::Parquet,
            ),
        );

        let mut data_file_writer = DataFileWriterBuilder::new(rolling_writer_builder)
            .build(None)
            .await
            .inspect_err(|err| error!(?err))?;

        data_file_writer
            .write(first_record_batch)
            .await
            .inspect_err(|err| debug!(?err))?;

        for write in writes.iter().skip(1) {
            let record_batch = self
                .schema_registry
                .as_arrow(topic, write.partition, write.batch, LakeHouseType::Iceberg)
                .await?;

            if record_batch.schema() != first_schema {
                return Err(Error::Message(format!(
                    "schema mismatch in transactional write for topic={topic}, partition={}, offset={}",
                    write.partition, write.offset
                )));
            }

            data_file_writer
                .write(record_batch)
                .await
                .inspect_err(|err| debug!(?err))?;
        }

        let data_files = data_file_writer
            .close()
            .await
            .inspect(|data_files| debug!(?data_files))
            .inspect_err(|err| debug!(?err))?;

        let commit_uuid = Uuid::now_v7();
        debug!(%commit_uuid);

        let tx = Transaction::new(&table);

        let tx = tx
            .fast_append()
            .set_commit_uuid(commit_uuid)
            .add_data_files(data_files)
            .apply(tx)
            .inspect_err(|err| debug!(?err))?;

        tx.commit(self.catalog.as_ref())
            .await
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
            .and(Ok(()))
    }
}

#[async_trait]
impl LakeHouse for Iceberg {
    async fn store(&self, write: LakeWriteRequest<'_>) -> Result<()> {
        let LakeWriteRequest {
            topic,
            partition,
            offset,
            inflated,
            config,
        } = write;

        let _ = config;

        let writes = [TransactionWrite {
            partition,
            offset,
            batch: inflated,
        }];

        self.store_transaction(topic, &writes).await
    }

    async fn maintain(&self) -> Result<()> {
        Ok(())
    }

    async fn lake_type(&self) -> Result<LakeHouseType> {
        Ok(LakeHouseType::Iceberg)
    }

    async fn stage_transactional(&self, request: StageTransactionalRequest<'_>) -> Result<()> {
        let StageTransactionalRequest { transaction, write } = request;

        let LakeWriteRequest {
            topic,
            partition,
            offset,
            inflated,
            config: _config,
        } = write;

        let mut staged = self.staged_writes.lock().map_err(|err| {
            Error::Message(format!("unable to lock iceberg staged writes: {err}"))
        })?;

        staged
            .entry(TxnKey::new(
                transaction.transaction_id,
                transaction.producer_id,
                transaction.producer_epoch,
            ))
            .or_default()
            .push(PendingWrite {
                topic: topic.to_owned(),
                partition,
                offset,
                batch: inflated.clone(),
            });

        Ok(())
    }

    async fn finalize_transaction(&self, request: FinalizeTransactionRequest<'_>) -> Result<()> {
        let key = TxnKey::new(
            request.transaction.transaction_id,
            request.transaction.producer_id,
            request.transaction.producer_epoch,
        );

        let writes = self
            .staged_writes
            .lock()
            .map_err(|err| Error::Message(format!("unable to lock iceberg staged writes: {err}")))?
            .remove(&key);

        if !request.committed {
            return Ok(());
        }

        let Some(mut writes) = writes else {
            warn!(
                transaction_id = request.transaction.transaction_id,
                producer_id = request.transaction.producer_id,
                producer_epoch = request.transaction.producer_epoch,
                "missing staged iceberg writes for committed transaction; treating as idempotent finalize"
            );

            return Ok(());
        };

        writes.sort_by(|lhs, rhs| {
            lhs.topic
                .cmp(&rhs.topic)
                .then(lhs.partition.cmp(&rhs.partition))
                .then(lhs.offset.cmp(&rhs.offset))
        });

        let mut by_topic: BTreeMap<String, Vec<TransactionWrite<'_>>> = BTreeMap::new();

        for write in &writes {
            by_topic
                .entry(write.topic.clone())
                .or_default()
                .push(TransactionWrite {
                    partition: write.partition,
                    offset: write.offset,
                    batch: &write.batch,
                });
        }

        for (topic, transaction_writes) in by_topic {
            self.store_transaction(topic.as_str(), &transaction_writes)
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lake::TransactionRef;
    use dotenv::dotenv;
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use rand::{distr::Alphanumeric, prelude::*, rng};
    use std::{env::var, fs::File, marker::PhantomData, str::FromStr as _, sync::Arc, thread};
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    pub(crate) fn alphanumeric_string(length: usize) -> String {
        rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect()
    }

    fn init_tracing() -> Result<DefaultGuard> {
        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(
                    EnvFilter::from_default_env()
                        .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
                )
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    async fn memory_iceberg(namespace: String) -> Result<Iceberg> {
        let schema_registry = Registry::from_str("memory://")?;

        Iceberg::new(
            Builder::<PhantomData<Url>, PhantomData<Url>, PhantomData<Registry>>::default()
                .location(Url::parse("memory://")?)
                .catalog(Url::parse("memory://")?)
                .schema_registry(schema_registry)
                .namespace(Some(namespace)),
        )
        .await
    }

    #[tokio::test]
    async fn finalize_committed_missing_staged_writes_is_noop() -> Result<()> {
        let lake = memory_iceberg(alphanumeric_string(8)).await?;

        lake.finalize_transaction(FinalizeTransactionRequest {
            transaction: TransactionRef {
                transaction_id: "txn-missing",
                producer_id: 1,
                producer_epoch: 0,
            },
            committed: true,
        })
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn finalize_abort_clears_staged_state() -> Result<()> {
        let lake = memory_iceberg(alphanumeric_string(8)).await?;
        let tx_key = TxnKey::new("txn-abort", 2, 0);

        {
            let mut guard = lake.staged_writes.lock()?;
            _ = guard.insert(
                tx_key.clone(),
                vec![PendingWrite {
                    topic: "topic-a".to_owned(),
                    partition: 0,
                    offset: 0,
                    batch: Batch::default(),
                }],
            );
        }

        lake.finalize_transaction(FinalizeTransactionRequest {
            transaction: TransactionRef {
                transaction_id: "txn-abort",
                producer_id: 2,
                producer_epoch: 0,
            },
            committed: false,
        })
        .await?;

        let guard = lake.staged_writes.lock()?;
        assert!(!guard.contains_key(&tx_key));

        Ok(())
    }

    #[tokio::test]
    async fn create_namespace() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
        let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
        let warehouse = var("ICEBERG_WAREHOUSE").ok();
        let namespace = alphanumeric_string(5);
        debug!(catalog_uri, location_uri, ?warehouse, namespace);

        let schema_registry = Registry::from_str("memory://")?;

        let lake = Iceberg::new(
            Builder::<PhantomData<Url>, PhantomData<Url>, PhantomData<Registry>>::default()
                .location(Url::parse(location_uri)?)
                .catalog(Url::parse(catalog_uri)?)
                .warehouse(warehouse.clone())
                .schema_registry(schema_registry)
                .namespace(Some(namespace.clone())),
        )
        .await?;

        let ident = lake.create_namespace().await?;
        assert_eq!(namespace, ident.to_url_string());

        Ok(())
    }

    #[tokio::test]
    async fn create_duplicate_namespace() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
        let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
        let warehouse = var("ICEBERG_WAREHOUSE").ok();
        let namespace = alphanumeric_string(5);
        debug!(catalog_uri, location_uri, ?warehouse, namespace);

        let schema_registry = Registry::from_str("memory://")?;

        {
            let lake = Iceberg::new(
                Builder::<PhantomData<Url>, PhantomData<Url>, PhantomData<Registry>>::default()
                    .location(Url::parse(location_uri)?)
                    .catalog(Url::parse(catalog_uri)?)
                    .warehouse(warehouse.clone())
                    .schema_registry(schema_registry.clone())
                    .namespace(Some(namespace.clone())),
            )
            .await?;

            let ident = lake.create_namespace().await?;
            assert_eq!(namespace, ident.to_url_string());
        }

        {
            let lake = Iceberg::new(
                Builder::<PhantomData<Url>, PhantomData<Url>, PhantomData<Registry>>::default()
                    .location(Url::parse(location_uri)?)
                    .catalog(Url::parse(catalog_uri)?)
                    .warehouse(warehouse)
                    .schema_registry(schema_registry)
                    .namespace(Some(namespace.clone())),
            )
            .await?;

            let ident = lake.create_namespace().await?;
            assert_eq!(namespace, ident.to_url_string());
        }

        Ok(())
    }

    #[tokio::test]
    async fn create_table() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
        let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
        let warehouse = var("ICEBERG_WAREHOUSE").ok();
        let namespace = alphanumeric_string(5);

        debug!(catalog_uri, location_uri, ?warehouse, namespace);

        let schema_registry = Registry::from_str("memory://")?;

        let lake_house = Iceberg::new(
            Builder::<PhantomData<Url>, PhantomData<Url>, PhantomData<Registry>>::default()
                .location(Url::parse(location_uri)?)
                .catalog(Url::parse(catalog_uri)?)
                .namespace(Some(namespace.clone()))
                .schema_registry(schema_registry)
                .warehouse(warehouse.clone()),
        )
        .await?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .build()?;

        let table_name = alphanumeric_string(5);

        let table = lake_house.load_or_create_table(&table_name, schema).await?;
        assert_eq!(table_name, table.identifier().name());
        assert_eq!(namespace, table.identifier().namespace().to_url_string());

        Ok(())
    }

    #[tokio::test]
    async fn create_duplicate_table() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
        let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
        let warehouse = var("ICEBERG_WAREHOUSE").ok();
        let namespace = alphanumeric_string(5);
        let table_name = alphanumeric_string(5);

        debug!(catalog_uri, location_uri, ?warehouse, namespace, table_name);

        let schema_registry = Registry::from_str("memory://")?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .build()?;

        {
            let lake_house = Iceberg::new(
                Builder::<PhantomData<Url>, PhantomData<Url>, PhantomData<Registry>>::default()
                    .location(Url::parse(location_uri)?)
                    .catalog(Url::parse(catalog_uri)?)
                    .warehouse(warehouse.clone())
                    .schema_registry(schema_registry.clone())
                    .namespace(Some(namespace.clone())),
            )
            .await?;

            let table = lake_house
                .load_or_create_table(&table_name, schema.clone())
                .await?;
            assert_eq!(table_name, table.identifier().name());
            assert_eq!(namespace, table.identifier().namespace().to_url_string());
        }

        {
            let lake_house = Iceberg::new(
                Builder::<PhantomData<Url>, PhantomData<Url>, PhantomData<Registry>>::default()
                    .location(Url::parse(location_uri)?)
                    .catalog(Url::parse(catalog_uri)?)
                    .namespace(Some(namespace.clone()))
                    .schema_registry(schema_registry)
                    .warehouse(warehouse),
            )
            .await?;

            let table = lake_house.load_or_create_table(&table_name, schema).await?;
            assert_eq!(table_name, table.identifier().name());
            assert_eq!(namespace, table.identifier().namespace().to_url_string());
        }

        Ok(())
    }

    #[test]
    fn url_parse() -> Result<()> {
        let uri = Url::parse("http://localhost:8181")?;
        assert_eq!("http://localhost:8181/", uri.as_str());
        assert_eq!("http", uri.scheme());
        assert!(uri.has_host());
        assert_eq!(Some("localhost"), uri.host_str());
        assert_eq!(Some(8181), uri.port());
        assert_eq!("/", uri.path());

        let uri = Url::parse("http://localhost:8181/catalog")?;
        assert_eq!("http://localhost:8181/catalog", uri.as_str());
        assert_eq!("http", uri.scheme());
        assert!(uri.has_host());
        assert_eq!(Some("localhost"), uri.host_str());
        assert_eq!(Some(8181), uri.port());
        assert_eq!("/catalog", uri.path());

        Ok(())
    }
}
