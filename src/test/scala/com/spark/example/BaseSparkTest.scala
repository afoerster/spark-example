package com.spark.example

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.concurrent.duration._

trait BaseSparkTest extends FunSuite with BeforeAndAfterAll {

  /*
    trip_id,duration,start_date,start_station,start_terminal,end_date,end_station,end_terminal,bike_number,subscriber_type,zip_code

   */

  val TRIP_DATA_CSV_PATH     = "data/201508_trip_data.csv"
  val STATION_DATA_JSON_PATH = "data/201508_station_data.json"
  val TRIP_DATA_JSON_PATH    = "data/201508_trip_data.json"

  val STATION_DATA_CSV_PATH     = "data/201508_station_data.csv"
  val STATION_DATA_PARQUET_PATH = "data/201508_station_data.parquet"
  val TRIP_DATA_PARQUET_PATH    = "data/201508_trip_data.parquet"

  val BLOCK_ON_COMPLETION = false

  /**
   * Keep the Spark Context running so the Spark UI can be viewed after the test has completed.
   */
  override def afterAll: Unit = {
    if (BLOCK_ON_COMPLETION) {
      // open SparkUI at http://localhost:4040
      Thread.sleep(10000.seconds.toMillis)
    }
  }
}
