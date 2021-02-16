-- task 2.1 
-- Most popular destinations by amount of passengers
-- parameter "k" limits the output for each pick-up zone

CREATE TABLE metrics.pop_dest_by_passengers (
	months varchar(10) NULL,
	pick_up varchar(255) NULL,
	drop_off varchar(255) NULL,
	passenger_count int8 NULL,
	drank int8 NULL
);

insert into metrics.pop_dest_by_passengers
select
	b.months,
	b.pick_up,
	b.drop_off,
	b.passenger_count,
	b.drank
from (select 
		a.months,
		a.pick_up,
		a.drop_off,
		a.passenger_count,
		dense_rank() over(partition by a.months, a.pick_up
			order by a.months, a.pick_up, a.passenger_count desc) as drank
	from (
		select 
			to_char(t.lpep_pickup_datetime, 'YYYY-MM') as months,
			zp.zone as pick_up,
			zd.zone as drop_off,
			sum(t.passenger_count) as passenger_count
		from stage.taxi_trips t
		join stage.taxi_zone zp
			on t.pulocation_id = zp.location_id
		join stage.taxi_zone zd
			on t.dolocation_id = zd.location_id 
		where 1=1
		group by months, pick_up, drop_off
		order by months, pick_up, passenger_count desc
	) a 
) b
where b.drank <= 10 -- {params.k}
order by b.months, b.pick_up, b.passenger_count desc
;