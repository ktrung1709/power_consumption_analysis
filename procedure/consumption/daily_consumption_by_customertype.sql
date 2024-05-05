insert into serving.daily_consumption_by_customer_type (customer_type, date, consumption)
select c.customer_type, dc."date", sum(consumption) as consumption from serving.daily_consumption_by_customer dc
inner join cmis.customer c on c.customer_id = dc.customer_id
group by c.customer_type, dc."date"