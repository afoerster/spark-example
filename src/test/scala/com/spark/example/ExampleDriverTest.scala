package com.spark.example

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._

class ExampleDriverTest extends FunSuite {
  // Build a `SparkSession` to be used during tests
  val spark =
    SparkSession
      .builder()
      .appName("DataFrame Examples")
      .master("local[*]") // Spark runs in 'local' mode using all cores
      .config("spark.executor.instances", "3") // 3 executors
      .config("spark.executor.cores", "1") // 1 core each
      .getOrCreate()

  test("Reads trip data") {
    val tripData = ExampleDriver.readData(spark, "data/201508_trip_data.csv")
    assert(tripData.count === 354152)
  }

  test("Multiplies dock count by 2") {
    val tripData = ExampleDriver.readData(spark, "data/201508_trip_data.csv")
    val doubledCount = ExampleDriver.doubleTripDuration(spark, tripData)
    val originalDockCount = ExampleDriver.aggregateDuration(spark, tripData)
    val doubledDockCount = ExampleDriver.aggregateDuration(spark, doubledCount)
    assert(originalDockCount * 2 === doubledDockCount)
  }
}
