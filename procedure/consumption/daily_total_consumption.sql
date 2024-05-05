insert into serving.daily_total_consumption (date, consumption)
select date, sum(consumption) as consumption
from serving.daily_consumption_by_customer_type
group by date