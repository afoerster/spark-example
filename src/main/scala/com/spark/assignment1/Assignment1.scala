package com.spark.assignment1

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object Assignment1 {

  private val timestampFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("M/d/yyyy H:mm")

  /**
   * Helper function to print out the contents of an RDD
   * @param label Label for easy searching in logs
   * @param theRdd The RDD to be printed
   * @param limit Number of elements to print
   */
  private def printRdd[_](label: String, theRdd: RDD[_], limit: Integer = 20) = {
    val limitedSizeRdd = theRdd.take(limit)
    println(s"""$label ${limitedSizeRdd.toList.mkString(",")}""")
  }

  def problem1(tripData: RDD[Trip]): Long = {
    tripData.map(x => x.duration).max()
  }

  def problem2(trips: RDD[Trip]): Long = {
    trips.filter(x => x.start_station.equals("San Antonio Shopping Center")).count()
  }

  def problem3(trips: RDD[Trip]): Seq[String] = {
    trips.groupBy(x => x.subscriber_type).map(x => x._1).distinct().collect()
  }

  def problem4(trips: RDD[Trip]): String = {
    trips.map(x => (x.zip_code, 1)).reduceByKey((x, y) => x + y).reduce((x, y) => if (x._2 > y._2) x else y)._1
  }

  def problem5(trips: RDD[Trip]): Long = {
    trips.filter(x => parseTimestamp(x.start_date).getDayOfYear != parseTimestamp(x.end_date).getDayOfYear).count()
  }

  def problem6(trips: RDD[Trip]): Long = {
    trips.count()
  }

  def problem7(trips: RDD[Trip]): Double = {
    val cachedTrips = trips.cache()
    val count = cachedTrips.count()
    val overnightTrips = cachedTrips
      .filter(x => parseTimestamp(x.start_date).getDayOfYear != parseTimestamp(x.end_date).getDayOfYear)
      .count()
    overnightTrips / count.toDouble
  }

  def problem8(trips: RDD[Trip]): Double = {
    trips.map(x => x.copy(duration = x.duration * 2)).map(_.duration).sum()
  }

  def problem9(trips: RDD[Trip], stations: RDD[Station]): (Double, Double) = {
    val filteredTrips = trips.filter(_.trip_id == "913401")
    val tripsPair = filteredTrips.map(t => (t.start_station, t))
    val stationPair = stations.map(s => (s.name, s))
    val joined: RDD[(String, (Trip, Station))] = tripsPair.join(stationPair)
    joined.map(x => (x._2._2.lat, x._2._2.lon)).first()
  }

  def problem10(trips: RDD[Trip], stations: RDD[Station]): Array[(String, Long)] = {
    val tripsPair = trips.map(t => (t.start_station, t))
    val stationPair = stations.map(s => (s.name, s))
    val joined: RDD[(String, (Trip, Station))] = tripsPair.join(stationPair)
    val grouped: RDD[(String, Iterable[(Trip, Station)])] = joined.groupByKey()
    val result: Array[(String, Long)] = grouped.map(g => (g._1, g._2.map(x => x._1.duration).sum)).collect()
    result
  }

  /*
   Dataframes
   */

  def dfProblem11(trips: DataFrame): DataFrame = {
    trips.select("trip_id")
  }

  def dfProblem12(trips: DataFrame): DataFrame = {
    trips.filter(col("start_station") === "Harry Bridges Plaza (Ferry Building)")
  }

  def dfProblem13(trips: DataFrame): Long = {
    trips.agg(sum("duration")).first().getLong(0)
  }

  private def parseTimestamp(timestamp: String) = LocalDateTime.from(timestampFormat.parse(timestamp))

}
