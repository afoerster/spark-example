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
    ???
  }

  def problem2(trips: RDD[Trip]): Long = {
    ???
  }

  def problem3(trips: RDD[Trip]): Seq[String] = {
    ???
  }

  def problem4(trips: RDD[Trip]): String = {
    ???
  }

  def problem5(trips: RDD[Trip]): Long = {
    ???
  }

  def problem6(trips: RDD[Trip]): Long = {
    trips.count()
  }

  def problem7(trips: RDD[Trip]): Double = {
    ???
  }

  def problem8(trips: RDD[Trip]): Double = {
    ???
  }

  def problem9(trips: RDD[Trip], stations: RDD[Station]): (Double, Double) = {
    ???
  }

  def problem10(trips: RDD[Trip], stations: RDD[Station]): Array[(String, Long)] = {
    ???
  }

  /*
   Dataframes
   */

  def dfProblem11(trips: DataFrame): DataFrame = {
    ???
  }

  def dfProblem12(trips: DataFrame): DataFrame = {
    ???
  }

  def dfProblem13(trips: DataFrame): Long = {
    ???
  }

  // Helper function to parse the timestamp format used in the trip dataset.
  private def parseTimestamp(timestamp: String) = LocalDateTime.from(timestampFormat.parse(timestamp))
}
