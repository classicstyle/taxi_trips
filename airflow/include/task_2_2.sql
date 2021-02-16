-- task 2.2
-- Most popular destinations by amount of rides

CREATE TABLE metrics.pop_dest_by_rides (
	months varchar(10) NULL,
	pick_up varchar(255) NULL,
	drop_off varchar(255) NULL,
	rides_count int8 NULL,
	drank int8 NULL
);

insert into metrics.pop_dest_by_rides
select 
	a.months,
	a.pick_up,
	a.drop_off,
	a.rides_count,
	dense_rank() over(partition by a.months, a.pick_up
		order by a.months, a.pick_up, a.rides_count desc) as drank
from (
	select 
		to_char(t.lpep_pickup_datetime, 'YYYY-MM') as months,
		-- date_trunc('month', t.lpep_pickup_datetime) as months,
		zp.borough as pick_up,
		zd.borough as drop_off,
		count(1) as rides_count
	from stage.taxi_trips t
	join stage.taxi_zone zp
		on t.pulocation_id = zp.location_id
	join stage.taxi_zone zd
		on t.dolocation_id = zd.location_id 
	where 1=1 --params.p_date
	group by months, pick_up, drop_off
	order by months, pick_up, rides_count desc
) a
order by a.months, a.pick_up, a.rides_count desc
;