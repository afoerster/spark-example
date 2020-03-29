# Spark Example

This project contains for running and testing applications locally using Apache Spark.

Apache Spark is fully testable locally using standard unit testing frameworks.

Unit testing is accepted engineering best practice, but is often not used.
Instead, a developer will either work from the Spark shell, copying code back
to their IDE when it works and piece it together, or develop locally and build 
a jar to deploy to the cluster for testing on the full dataset. These options are
time consuming, error prone, and not scalable beyond a single developer.

Writing unit tests allows you to test a variety of data, catch regressions, and 
greatly reduce the iteration cycle so it's possible to get more done with better quality.

## Project Overview

- Language: [Scala](https://www.scala-lang.org/)
- Framework: [Apache Spark](https://spark.apache.org/)
- Build tool: [SBT](https://www.scala-sbt.org/) 
- Testing Framework: [Scalatest](http://www.scalatest.org/)

## Code Overview

### Driver

`ExampleDriver` is a Spark `Driver` (or coordinator) that will run a Spark application

It defines: 
- a 'main' class that allows the Spark appliction
to be run using `spark-submit` 
- a function `readData` to load data from a datasource
- a function `process` to apply transformations to the data

Functions `readData' and `process` take as an argument a `Spark` object. This Spark object
will be different if the `ExampleDriver` is run on a real cluster or in the unit tests in the project.

### Test

`ExampleDriverTest` is a test for the Spark driver. It contains two tests,
one to assert we can read data and the other that we can apply a transformation
to the data.

## IDE Setup

- Download [Intellij IDEA Community Edition](https://www.jetbrains.com/idea/download/#section=mac)
- Install the `Scala` plugin in intellij ([plugin install instructions](https://www.jetbrains.com/help/idea/managing-plugins.html))
- From Intellij, open the `build.sbt` file in this directory with 'File' -> 'Open'. Opening the `build.sbt` file will ensure Intellij loads the project correctly
- When prompted, choose 'Open as Project'

## Running Tests

### From Intellij

Right click on `ExampleDriverTest` and choose `Run 'ExampleDriverTest'`

### From the command line

On Unix systems, test can be run:

```shell script
$ ./sbt test
```

or on Windows systems:

```shell script
C:\> ./sbt.bat test
```

## Configuring Logging

Spark uses log4j 1.2 for logging. Logging levels can be configured in the file `src/test/resources/log4j.properties`

Spark logging can be verbose, for example, it will tell you when each task starts and finishes as well
as resource cleanup messages. This isn't always useful or desired during regular development. To reduce the verbosity of logs,
change the line `log4j.logger.org.apache.spark=INFO` to `log4j.logger.org.apache.spark=WARN`

## Scala Worksheets

The worksheet `src/test/scala/com/spark/example/playground.sc` is a good place to try out Scala code. Add your code
to the left pane of the worksheet, click the 'play' button, and the result will display in the right pane.

Note: The worksheet will not work for Spark code.

## Documentation

* RDD: https://spark.apache.org/docs/latest/rdd-programming-guide.html
* Batch Structured APIs: https://spark.apache.org/docs/latest/sql-programming-guide.html
