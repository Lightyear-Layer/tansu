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

use crate::common::{Error, init_tracing};
use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
use rand::{distr::Alphanumeric, prelude::*, rng};
use tansu_sans_io::{EndTxnRequest, ErrorCode, InitProducerIdRequest};
use tansu_storage::{InitProducerIdService, StorageContainer, TxnEndService};
use url::Url;

mod common;

fn random_txn_id() -> String {
    rng()
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect()
}

#[tokio::test]
async fn end_txn_commit_returns_none_error() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;

    let storage = StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(Url::parse("memory://tansu/")?)
        .build()
        .await?;

    let init_producer = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(InitProducerIdService)
    };

    let end_txn = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(TxnEndService)
    };

    let transactional_id = random_txn_id();

    let producer = init_producer
        .serve(
            Context::default(),
            InitProducerIdRequest::default()
                .transactional_id(Some(transactional_id.clone()))
                .transaction_timeout_ms(30_000)
                .producer_id(Some(-1))
                .producer_epoch(Some(-1)),
        )
        .await?;

    let response = end_txn
        .serve(
            Context::default(),
            EndTxnRequest::default()
                .transactional_id(transactional_id)
                .producer_id(producer.producer_id)
                .producer_epoch(producer.producer_epoch)
                .committed(true),
        )
        .await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

    Ok(())
}

#[tokio::test]
async fn end_txn_abort_returns_none_error() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;

    let storage = StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(Url::parse("memory://tansu/")?)
        .build()
        .await?;

    let init_producer = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(InitProducerIdService)
    };

    let end_txn = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(TxnEndService)
    };

    let transactional_id = random_txn_id();

    let producer = init_producer
        .serve(
            Context::default(),
            InitProducerIdRequest::default()
                .transactional_id(Some(transactional_id.clone()))
                .transaction_timeout_ms(30_000)
                .producer_id(Some(-1))
                .producer_epoch(Some(-1)),
        )
        .await?;

    let response = end_txn
        .serve(
            Context::default(),
            EndTxnRequest::default()
                .transactional_id(transactional_id)
                .producer_id(producer.producer_id)
                .producer_epoch(producer.producer_epoch)
                .committed(false),
        )
        .await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

    Ok(())
}
