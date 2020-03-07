ThisBuild / scalaVersion     := "2.12.10"
ThisBuild / version          := "0.1.0"
ThisBuild / organization     := "com.spark.example"
ThisBuild / organizationName := "example"

val sparkVersion = "2.4.1"

lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8"
lazy val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
lazy val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion

lazy val root = (project in file("."))
  .settings(
    name := "spark-example",
    libraryDependencies ++= Seq(scalaTest % Test, sparkCore, sparkSql)
  )
