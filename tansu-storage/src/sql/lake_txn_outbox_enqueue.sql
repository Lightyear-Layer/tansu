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
