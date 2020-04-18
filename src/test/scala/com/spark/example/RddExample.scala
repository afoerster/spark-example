package com.spark.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

class RddExample extends BaseSparkTest {

  val spark =
    SparkSession
      .builder()
      .appName("RDD Examples")
      .master("local[4]") // Spark runs in 'local' mode using 4 cores
      .getOrCreate()

  override val BLOCK_ON_COMPLETION: Boolean = true

  val sc = spark.sparkContext // reference to the SparkContext. Pre-spark 2.0 API Useful for interacting with RDDs

  private def printRdd[_](label: String, theRdd: RDD[_], limit: Integer = 20) = {
    val limitedSizeRdd = theRdd.take(limit)
    println(s"""$label ${limitedSizeRdd.toList.mkString(",")}""")
  }

  private def printResult[A](label: String, result: A): Unit = {
    println(s"""$label $result""")
  }

  val rdd = spark.range(10).rdd

  test("Laziness") {

    // This is never executed
    val notExecutedResult: RDD[Unit] = spark
      .range(1, 100)
      .rdd
      .map(x => x - 1)
      .repartition(10000)
      .map(n => println(s"This is never executed! $n"))

    println(notExecutedResult)

    val executedResult: Array[Long] = spark
      .range(101, 200)
      .rdd
      .map(x => x - 1)
      .repartition(10000)
      .map(n => {
        println(s"processing $n");
        n
      })
      .collect

    println(executedResult.toSeq)

  }

  test("Count elements in an RDD") {
    val result = rdd.count()
    printResult("Count", result)
  }

  test("Map rdd") {
    val resultRdd = rdd.map(n => n + 1)
    printRdd("map rdd", resultRdd)
  }

  test("Closure") {
    val freeVariable = 10

    val result = rdd.map(n => n + freeVariable)
    printRdd("closure", result)
  }

  test("Filter an RDD") {
    val result = rdd.filter(n => n % 2 == 0)

    printRdd("Filter RDD", result)
  }

  test("Reduce an RDD") {
    val result = rdd.reduce((left, right) => left + right)

    printResult("Reduce RDD", result)
  }

  test("Join RDDs") {
    val rdd1 = spark.sparkContext.parallelize(Seq((1, "foo"), (2, "bar")))
    val rdd2 = spark.sparkContext.parallelize(Seq((1, "bill"), (2, "george")))

    rdd1.map(x => x.copy(_2 = x._2.toUpperCase))
    rdd2.map(x => x.copy(_2 = x._2.toUpperCase()))
    val result = rdd1.join(rdd2)

    printRdd("Join RDDs", result)
  }

  test("Tasks, Stages, Jobs") {
    val stage1_0 = spark.range(10).rdd

    // task 1 is multiplying element by t
    val stage1_1 = stage1_0.map(x => x * 2)

    // task 2 is adding 3 to each element
    val stage1_2 = stage1_1.map(x => x + 3).cache()

    // stage ends here. Boundary is shuffle (repartition)
    val stage2_0 = stage1_2.repartition(10)

    val stage2_1 = stage2_0.filter(x => x % 3 == 0)

    // job concluded by an action
    val jobResult = stage2_1.reduce((l, r) => l + r)

    assert(48 == jobResult)
  }

  test("Lineage example") {
    val stage1_0 = spark.range(1, 10000).rdd
    val stage1_1 = stage1_0.map(x => x * 2) // fails here, start from stage 1_0
    val stage1_2 = stage1_1.map(x => x + 3) // fails here, start from stage 1_0
    val stage2_0 = stage1_2.repartition(10).cache() //fails here, start from stage 1_0
    val stage2_1 = stage2_0.filter(x => x % 3 == 0) // fails here, start from stage 2_0

    // job concluded by an action
    val jobResult = stage2_1.reduce((l, r) => l + r)

    assert(48 == jobResult)
  }

  test("Cache") {
    val freeVariable = 10

    val result = rdd.map(n => n + freeVariable).persist(StorageLevel.MEMORY_ONLY)
    printRdd("closure", result)

    result.unpersist()
  }

  test("Should have cached this RDD") {
    println(sc.defaultParallelism)
    println(sc.defaultMinPartitions)
    val result = rdd.map { n =>
      println(s"**$n"); n * 2
    }

    printRdd("no cache V1", result)
    printRdd("no cache V2", result)
  }
}
