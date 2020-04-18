package com.spark.example

import com.spark.assignment1.{Station, Trip}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}

class DatasetExample extends BaseSparkTest {

  val spark =
    SparkSession
      .builder()
      .appName("DataSet Examples")
      .master("local[*]") // Spark runs in 'local' mode using all cores
      .config("spark.executor.instances", "3")
      .getOrCreate()

  implicit val tripEncoder: Encoder[Trip] = Encoders.product[Trip]

  implicit val stationEncoder: Encoder[Station] = Encoders.product[Station]

  test("Load data") {
    loadTrips.show()
  }

  test("Load station") {
    loadStations.show()
  }

  test("Map trips") {
    val trips = loadTrips
    val result: Dataset[Trip] = trips.map(x => x.copy(duration = x.duration * 2))

    result.show()
  }

  test("Filter trips") {
    val trips = loadTrips
    val result = trips.filter(trip => trip.duration > 600).join(loadTrips)
    println(result)
  }

  test("Aggregate trips") {
    val trips = loadTrips
    trips.groupBy("start_date").sum("duration").show()
  }

  private def loadTrips: Dataset[Trip] = {
    spark.read.parquet(TRIP_DATA_PARQUET_PATH).as[Trip].cache()
  }

  private def loadStations: Dataset[Station] = {
    spark.read.parquet(STATION_DATA_PARQUET_PATH).withColumnRenamed("long", "lon").as[Station].cache()
  }
}
