create table __tmp(val int)

declare @i int = 0
while @i < 100000
begin
insert into __tmp(val)
select @i

set @i = @i+1
END
GO

--duplicates
select value, count(*)
from NEW__ASYNC_PROCESSING_AUTO_COMMIT_CONSUMER_44
group by value
having count(*) > 1


--missing
select p.* 
from __tmp p
where not exists(
	select 1
	from NEW__ASYNC_PROCESSING_AUTO_COMMIT_CONSUMER_44 c
	where c.[value] = p.val
)



--missing incomplete test
select partition, max(offset), count(*)-1
from RESILIENCE_TEST__SYNC_PROCESSING_SYNC_COMMIT_CONSUMER_3
group by partition

