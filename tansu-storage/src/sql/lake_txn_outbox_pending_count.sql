select count(*)
from lake_txn_outbox o
join cluster c on c.id = o.cluster
where c.name = $1
    and o.status in ('pending', 'failed', 'in_progress');
