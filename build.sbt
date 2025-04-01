import Dependencies._

ThisBuild / scalaVersion     := "2.12.10"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "Data-Cleaning-and-Transformation-Pipeline",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.1.1",
      "org.apache.spark" %% "spark-sql" % "3.1.1"
    )
  )

