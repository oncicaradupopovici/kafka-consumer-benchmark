select datediff(MILLISECOND, MIN(RecordStamp), MAX(RecordStamp)) / cast(count(*) as decimal)
from NEW__ASYNC_PROCESSING_AUTO_COMMIT_CONSUMER_32

select count(*)
from NEW__ASYNC_PROCESSING_AUTO_COMMIT_CONSUMER_32