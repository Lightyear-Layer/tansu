-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

insert into lake_txn_outbox (
    cluster,
    transaction_id,
    producer_id,
    producer_epoch,
    committed,
    status,
    attempt_count,
    next_attempt_at,
    last_error
)
select
    c.id,
    $2,
    $3,
    $4,
    $5,
    'pending',
    0,
    current_timestamp,
    null
from cluster c
where c.name = $1
on conflict (cluster, transaction_id, producer_id, producer_epoch)
do nothing;
