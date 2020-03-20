package com.spark.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ExampleDriver {

  def main(args: Array[String]): Unit = {
    val distributedSparkSession =
      SparkSession.builder().appName("Testing Example").getOrCreate()

    val data = readData(distributedSparkSession, "data/201508_trip_data.csv")
    val result = doubleTripCount(distributedSparkSession, data)
    result.write.parquet("/target/testing-example-data")
  }

  def readData(sparkSession: SparkSession, path: String): DataFrame = {
    val csvReadOptions =
      Map("inferSchema" -> true.toString, "header" -> true.toString)

    val stationData =
      sparkSession.read.options(csvReadOptions).csv(path)

    stationData
  }

  def doubleTripCount(sparkSession: SparkSession, data: DataFrame): DataFrame = {
    data.select(
      col("end_terminal"),
      col("start_date"),
      col("subscriber_type"),
      col("start_terminal"),
      col("end_station"),
      col("trip_id"),
      expr("duration * 2") as "duration",
      col("bike_number"),
      col("end_date"),
      col("start_station"),
      col("zip_code")
    )
  }

  def aggregateDuration(sparkSession: SparkSession, data: DataFrame): Long = {
    data.agg(sum("duration")).first.get(0).asInstanceOf[Long]
  }
}
