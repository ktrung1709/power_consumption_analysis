insert into serving.monthly_consumption_by_location(location, latitude, longitude, consumption, month, year)
select location, latitude, longitude, sum(consumption) as consumption, DATE_PART(month,"date") as month, DATE_PART(year,"date") as year
from serving.daily_consumption_by_location
group by location, latitude, longitude, DATE_PART(month,"date"), DATE_PART(year,"date")