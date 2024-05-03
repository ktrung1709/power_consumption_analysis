insert into serving.monthly_total_consumption (month, year, consumption)
select month, year, sum(consumption) as consumption
from serving.monthly_consumption_by_customer_type
group by month, year