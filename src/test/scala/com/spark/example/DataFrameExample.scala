package com.spark.example

import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

class DataFrameExample extends BaseSparkTest {

  val spark =
    SparkSession
      .builder()
      .appName("DataFrame Examples")
      .master("local[*]") // Spark runs in 'local' mode using all cores
      .config("spark.executor.instances", "3")
      .config("spark.sql.parquet.filterPushdown", true)
      .getOrCreate()

  val csvReadOptions =
    Map("inferSchema" -> true.toString, "header" -> true.toString)

  // Create parquet datasets that are used in later tests
  override def beforeAll() {
    if (!Files.exists(Paths.get(STATION_DATA_PARQUET_PATH))) {
      val csvReadOptions =
        Map("inferSchema" -> true.toString, "header" -> true.toString)

      val stationData =
        spark.read.options(csvReadOptions).csv(STATION_DATA_CSV_PATH)
      val tripData = spark.read.options(csvReadOptions).csv(TRIP_DATA_CSV_PATH)

      stationData.write
        .mode(SaveMode.Ignore)
        .option("compression", "none")
        .parquet(STATION_DATA_PARQUET_PATH)
      tripData.write
        .mode(SaveMode.Overwrite)
        .partitionBy("zip_code")
        .parquet(TRIP_DATA_PARQUET_PATH)

      spark.read.parquet(TRIP_DATA_PARQUET_PATH).filter("zip_code = 55066").explain(true)
    }
  }

  test("Show data") {
    import spark.implicits._
    val data = Seq((1 ,"minnesota"), (2, "iowa"), (3, "wisconsin")).toDF
    data.show()
  }

  test("Select data") {
    val stationData =
      spark.read.options(Map("inferSchema" -> true.toString, "header" -> true.toString)).format("csv").load("data/201508_station_data.csv")

    stationData.show(1)

    stationData.select(stationData("station_id")).show(1)
  }

  test("Aggregate data") {
    val stationData =
      spark.read.parquet(STATION_DATA_PARQUET_PATH).cache()

    stationData.show(1)

    stationData.agg(sum("dockcount")).explain(true)
  }

  test("Explain") {
    val stationData =
      spark.read.parquet(STATION_DATA_PARQUET_PATH)

    stationData.agg(sum("dockcount")).explain(true)
  }

  test("filter data") {
    val stationData =
      spark.read.options(csvReadOptions).csv(STATION_DATA_CSV_PATH)

    stationData.filter(col("station_id").isin(1, 2, 3)).show(1)
  }

  test("csv to json") {
    val stationData =
      spark.read.options(csvReadOptions).csv(STATION_DATA_CSV_PATH)
    val tripData = spark.read.options(csvReadOptions).csv(TRIP_DATA_CSV_PATH)
    tripData.printSchema()
    stationData.printSchema()

    stationData.write.mode(SaveMode.Ignore).json(STATION_DATA_JSON_PATH)
    tripData.write.mode(SaveMode.Ignore).json(TRIP_DATA_JSON_PATH)
  }

  test("folder predicate pushdown") {
    spark.read.parquet(TRIP_DATA_PARQUET_PATH).filter("zip_code = 55066").explain(true)
  }

  test("show ") {
    val csvReadOptions =
      Map("inferSchema" -> true.toString, "header" -> true.toString)

    val stationData =
      spark.read.options(csvReadOptions).csv(STATION_DATA_CSV_PATH)

    stationData.show(10)
    println(s"Station data count: " + stationData.count())
  }

  test("join") {
    val stationData = readStationDataDf()
    val tripData = readTripDataDf().withColumn("name", col("start_station"))

    val joined = stationData.join(tripData, Seq("name"))

    joined.show()

    joined.explain(true)
    val joinedCount = joined.count()
    println(s"Total joined count: $joinedCount")
    joined.write.mode(SaveMode.Overwrite).parquet("target/joined.parquet")
  }

  test("sum") {
    val tripData = readTripDataDf().toDF()

    val startTimestampConversion =
      to_timestamp(col("start_date"), "mm/dd/YYYY HH:mm")
    val endTimestampConversion =
      to_timestamp(col("end_date"), "mm/dd/YYYY HH:mm")

    val timeStampsDF = tripData
      .withColumn("start_timestamp", startTimestampConversion)
      .withColumn("end_timestamp", endTimestampConversion)

    val timeSpan =
      timeStampsDF.withColumn("ride_time",
        unix_timestamp(
          col("end_timestamp"),
          "yyyy-MM-dd HH:mm:ss") - unix_timestamp(
          col("start_timestamp"),
          "yyyy-MM-dd HH:mm:ss"))

    val rideTimeSeconds = timeSpan
      .agg(sum("ride_time"))
      .first()
      .get(0)
      .asInstanceOf[Long]

    println(s"Ride time seconds: $rideTimeSeconds person/seconds")

    val rideTimeDays =
      TimeUnit.DAYS
        .convert(rideTimeSeconds, TimeUnit.SECONDS)

    println(s"Total ride time: $rideTimeDays person/days")
  }

  test("Group by") {
    import org.apache.spark.sql.functions._
    val tripData = readTripDataDf()
    val bikeUsage =
      tripData
        .groupBy(col("bike_number"))
        .agg(count(col("bike_number")).alias("rides"))

    val orderedUsages = bikeUsage.orderBy(desc("rides"))
    orderedUsages.show(10)
  }

  private def readStationDataDf(): DataFrame = {
    spark.read.parquet(STATION_DATA_PARQUET_PATH)
  }

  private def readTripDataDf(): DataFrame = {
    Timer.timed("reading parquet", spark.read.parquet(TRIP_DATA_PARQUET_PATH))
  }
}
