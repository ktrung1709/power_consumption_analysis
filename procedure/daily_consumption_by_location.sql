insert into serving.daily_consumption_by_location (location, latitude, longitude, consumption, date)
select e.location, e.latitude, e.longitude, sum(p.measure) as consumption, date(p.datetime_measured) as date
from public.power_consumption p
inner join cmis.electric_meter e
    on p.meter_id = e.meter_id
group by date(p.datetime_measured), e.location, e.latitude, e.longitude