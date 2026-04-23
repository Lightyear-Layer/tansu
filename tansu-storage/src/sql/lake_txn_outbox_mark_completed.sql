update lake_txn_outbox
set
    status = 'completed',
    completed_at = current_timestamp,
    last_error = null,
    last_updated = current_timestamp
where id = $1;
