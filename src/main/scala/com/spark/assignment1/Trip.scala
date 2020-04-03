package com.spark.assignment1

case class Trip(
    trip_id: String,
    duration: Long,
    start_date: String,
    start_station: String,
    start_terminal: Int,
    end_date: String,
    end_station: String,
    end_terminal: Int,
    bike_number: Int,
    subscriber_type: String,
    zip_code: String
)
