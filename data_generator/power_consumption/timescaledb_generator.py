import csv
import random
from datetime import datetime, timedelta
import psycopg2
import os

# Load meter IDs from CSV file
meter_ids = {}
with open('data/electric_meter_202404152150.csv', newline='', mode='r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for line in reader:
        if int(line['contract_id']) in range(1, 10001):
            meter_ids[line['meter_id']] = "residential"
        elif int(line['contract_id']) in range(10001, 10101):
            meter_ids[line['meter_id']] = "commercial"
        else:
            meter_ids[line['meter_id']] = "industrial"

# Function to generate random power consumption data for a specific meter ID
def generate_power_consumption(meter_id, customer_type, datetime_measured):
    if customer_type == "residential":
        if (datetime_measured.month-1)//3 + 1 == 1 or (datetime_measured.month-1)//3 + 1 == 3:
            if datetime_measured.weekday() in range(0, 5):
                if datetime_measured.hour in range(17, 24):
                    consumption_factor = 1
                else:
                    consumption_factor = 0.6
            else:
                if datetime_measured.hour in range(8, 24):
                    consumption_factor = 1
                else:
                    consumption_factor = 0.6
        else:
            if datetime_measured.weekday() in range(0, 5):
                if datetime_measured.hour in range(17, 24):
                    consumption_factor = 1.3
                else:
                    consumption_factor = 0.9
            else:
                if datetime_measured.hour in range(8, 24):
                    consumption_factor = 1
                else:
                    consumption_factor = 0.6
        measure = random.uniform(2.0, 5.0) * consumption_factor

    elif customer_type == "commercial":
        if datetime_measured.weekday() in range(0, 5):
            if datetime_measured.hour in range(8, 18):
                consumption_factor = 1
            else:
                consumption_factor = 0.6
        else:
            consumption_factor = 0.6
        measure = random.uniform(10.0, 100.0) * consumption_factor

    else:
        if datetime_measured.weekday() in range(0, 5):
            if datetime_measured.hour in range(8, 18):
                consumption_factor = 1
            else:
                consumption_factor = 0.6
        else:
            consumption_factor = 0.6
        measure = random.uniform(50.0, 500.0) * consumption_factor

    return {
        'meter_id': meter_id,
        'measure': measure,
        'datetime_measured': datetime_measured.strftime('%Y-%m-%d %H:%M:%S')
    }

def save_to_timescaledb(conn, data):
    with conn.cursor() as cursor:
        insert_query = """
            INSERT INTO public.power_consumption_streaming (meter_id, measure, datetime_measured)
            VALUES (%s, %s, %s)
        """
        for row in data:
            cursor.execute(insert_query, (row['meter_id'], row['measure'], row['datetime_measured']))
        conn.commit()

def main():
    # Connect to TimescaleDB
    conn = psycopg2.connect(
        dbname='speed_layer_db',
        user='postgres',
        password='password',
        host='localhost',
        port='5432'
    )

    hourly_data = []
    start_datetime = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)  # Start at 00:00 of the current date
    end_datetime = start_datetime.replace(hour=23)  # End at 23:00 of the current date

    current_datetime = start_datetime
    while current_datetime <= end_datetime:
        for meter_id, customer_type in meter_ids.items():
            power_consumption_data = generate_power_consumption(meter_id, customer_type, current_datetime)
            hourly_data.append(power_consumption_data)

        save_to_timescaledb(conn, hourly_data)
        hourly_data = []  # Reset hourly data for the next hour

        current_datetime += timedelta(hours=1)  # Move to the next hour
        print('Finished data for ' + current_datetime.strftime("%Y-%m-%d %H:%M:%S"))

    conn.close()

if __name__ == '__main__':
    main()
