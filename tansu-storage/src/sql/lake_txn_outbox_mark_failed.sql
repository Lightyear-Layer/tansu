update lake_txn_outbox
set
    status = 'failed',
    attempt_count = attempt_count + 1,
    next_attempt_at = current_timestamp + make_interval(secs => $2),
    last_error = $3,
    last_updated = current_timestamp
where id = $1;
