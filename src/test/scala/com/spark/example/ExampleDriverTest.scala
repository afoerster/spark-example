package com.spark.example

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._

class ExampleDriverTest extends FunSuite {
  val spark =
    SparkSession
      .builder()
      .appName("DataFrame Examples")
      .master("local[*]") // Spark runs in 'local' mode using all cores
      .config("spark.executor.instances", "3")
      .config("spark.executor.cores", "1")
      .getOrCreate()

  test("Reads trip data") {
    val tripData = ExampleDriver.readData(spark, "data/201508_trip_data.csv")
    assert(354154 == tripData.count)
  }

  test("Multiplies dock count by 2") {
    val tripData = ExampleDriver.readData(spark, "data/201508_trip_data.csv")
    val doubledCount = ExampleDriver.process(spark, tripData)
    val originalDockCount = tripData.agg(sum("duration")).first.get(0).asInstanceOf[Long]
    val doubledDockCount = doubledCount.agg(sum("duration")).first.get(0).asInstanceOf[Long]
    assert(originalDockCount * 2 == doubledDockCount )
  }

}
