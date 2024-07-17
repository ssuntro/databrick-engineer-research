select pickup_zip, sum(fare_amount) as total_fare from nyctaxi.trips
group by pickup_zip
