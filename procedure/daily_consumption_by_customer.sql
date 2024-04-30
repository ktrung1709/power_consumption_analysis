insert into serving.daily_consumption_by_customer (customer_id, customer_name, consumption, "date")
select cu.customer_id, cu.customer_name, sum(p.measure) as consumption, date(p.datetime_measured) as date
from public.power_consumption p
inner join cmis.electric_meter e
    on p.meter_id = e.meter_id
inner join cmis.contract c
    on c.contract_id = e.contract_id
inner join cmis.customer cu
    on cu.customer_id = c.customer_id
where DATE_PART(month, p.datetime_measured) = 7
group by date(p.datetime_measured), cu.customer_id, cu.customer_name
