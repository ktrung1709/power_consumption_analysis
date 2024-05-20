insert into dwh.fact_power_consumption (meter_id, date_id, customer_id, contract_id, time_of_day, consumption)
select a.meter_id, a.date_id, a.customer_id, a.contract_id, a.time_of_day, sum(a.measure) as consumption
FROM
(
SELECT
    pc.meter_id, CAST(TO_CHAR(pc.datetime_measured, 'yyyyMMdd') as INT) as date_id, c.customer_id, co.contract_id, pc.measure,
    CASE
        WHEN ( extract(hour from pc.datetime_measured) <= 11 AND extract(hour from pc.datetime_measured) > 9 ) OR
        ( extract(hour from pc.datetime_measured) <= 20 AND extract(hour from pc.datetime_measured) > 17 )
            THEN 'high'
        WHEN (extract(hour from pc.datetime_measured) = 23) OR
        ( extract(hour from pc.datetime_measured) <= 4 AND extract(hour from pc.datetime_measured) >= 0 )
            THEN 'low'
        ELSE 'normal'
    END as time_of_day
FROM
    "dev"."public"."power_consumption" pc
INNER JOIN cmis.electric_meter e on pc.meter_id = e.meter_id
inner join cmis.contract co on e.contract_id = co.contract_id
INNER join cmis.customer c on c.customer_id = co.customer_id
) as a
group by a.meter_id, a.date_id, a.customer_id, a.contract_id, a.time_of_day