package com.spark.example

object Timer {
  def timed[E](name: String, work: => E): E = {
    val start = System.currentTimeMillis()
    val result = work;
    val end = System.currentTimeMillis()
    val span = end - start

    println(s"$name ran for $span milliseconds")

    result
  }
}
