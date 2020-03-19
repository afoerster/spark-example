package com.spark.example.assignment1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

class Assignment1Test extends AnyFunSuite with Matchers {

  val TRIP_DATA_CSV_PATH = "data/201508_trip_data.csv"
  val STATION_DATA_CSV_PATH = "data/201508_station_data.csv"

  val spark =
    SparkSession
      .builder()
      .appName("DataFrame Examples")
      .master("local[*]") // Spark runs in 'local' mode using all cores
      .getOrCreate()

  implicit val tripEncoder: Encoder[Trip] = Encoders.product[Trip]

  implicit val stationEncoder: Encoder[Station] = Encoders.product[Station]

  val csvReadOptions =
    Map("inferSchema" -> true.toString, "header" -> true.toString)

  def tripDataDS: Dataset[Trip] = spark.read.options(csvReadOptions).csv(TRIP_DATA_CSV_PATH).as[Trip]
  def tripDataDF: DataFrame = tripDataDF.toDF()
  def tripDataRdd: RDD[Trip] = tripDataDS.rdd

  def stationDataDS: Dataset[Station] = spark.read.options(csvReadOptions).csv(STATION_DATA_CSV_PATH).as[Station]
  def stationDataDF: DataFrame = stationDataDS.toDF()
  def stationDataRdd: RDD[Station] = stationDataDS.rdd

  /**
   * We have to prove to the governor that our ride service is being used.
   * Find the total ride duration across all trips.
   */
  test("Find the longest trip (duration)") {
    Assignment1.problem1(tripDataRdd) must equal(17270400)
  }

  test("All trips starting at the 'San Antonio Shopping Center' station") {
    Assignment1.problem2(tripDataRdd) must equal(1069)
  }

  test("List out all subscriber types") {
    Assignment1.problem3(tripDataRdd).toSet must equal(Set("Customer", "Subscriber"))
  }

  /**
   *
   */
  test("Busiest zipcode") {
    Assignment1.problem4(tripDataRdd) must equal("94107")
  }

  /**
   * Some people keep their bikes for a long time. How many people keep their bikes overnight?
   */
  test("Trips that went overnight") {
    Assignment1.problem5(tripDataRdd) must equal(920)
  }

  /**
   * What is the total number of records in the dataset?
   */
  test("Get the dataset count") {
    Assignment1.problem6(tripDataRdd) must equal(354152)
  }

  /**
   * What percentage of people keep their bikes overnight at least on night?
   */
  test("Get the percentage of trips that went overnight") {
    Assignment1.problem7(tripDataRdd) must be(0.0025 +- .0003)
  }

  /**
   * The docks were miscalibrated and only counted half of a trip duration. Double the duration of each trip so
   * we can have an accurate measurement, and get new total duration.
   */
  test("Double the duration of each trip") {
    Assignment1.problem8(tripDataRdd) must equal (7.40909118E8)
  }

  /*
   Dataframes
   */
  test("Select Dataframe") {
    Assignment1.problem9(tripDataDF).schema.length must equal (1)
  }

  test("Count of all trips starting at 'Harry Bridges Plaza (Ferry Building)'") {
    Assignment1.problem10(tripDataDF) must equal (1)
  }

  test("Sum the duration of all trips") {
    Assignment1.problem11(tripDataDF) must equal (1)
  }

  test("Find the value of the longest trip") {
    Assignment1.problem12(tripDataDF) must equal (1)
  }

}
