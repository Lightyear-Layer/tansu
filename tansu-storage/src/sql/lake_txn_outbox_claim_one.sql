with candidate as (
    select
        o.id,
        o.transaction_id,
        o.producer_id,
        o.producer_epoch,
        o.committed,
        o.created_at,
        o.attempt_count
    from lake_txn_outbox o
    join cluster c on c.id = o.cluster
    where c.name = $1
        and o.status in ('pending', 'failed')
        and o.next_attempt_at <= current_timestamp
    order by o.created_at
    for update skip locked
    limit 1
)
update lake_txn_outbox o
set
    status = 'in_progress',
    last_error = null,
    last_updated = current_timestamp
from candidate
where o.id = candidate.id
returning
    candidate.id,
    candidate.transaction_id,
    candidate.producer_id,
    candidate.producer_epoch,
    candidate.committed,
    candidate.created_at,
    candidate.attempt_count;
