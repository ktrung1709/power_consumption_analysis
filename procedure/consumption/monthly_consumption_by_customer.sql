insert into serving.monthly_consumption_by_customer(customer_name, customer_id, consumption, month, year)
select customer_name, customer_id, sum(consumption) as consumption, DATE_PART(month,"date") as month, DATE_PART(year,"date") as year
from serving.daily_consumption_by_customer
group by customer_name, customer_id, DATE_PART(month,"date"), DATE_PART(year,"date")