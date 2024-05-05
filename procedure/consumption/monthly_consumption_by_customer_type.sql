insert into serving.monthly_consumption_by_customer_type(customer_type, consumption, month, year)
select customer_type, sum(consumption) as consumption, DATE_PART(month,"date") as month, DATE_PART(year,"date") as year
from serving.daily_consumption_by_customer_type
group by customer_type, DATE_PART(month,"date"), DATE_PART(year,"date")