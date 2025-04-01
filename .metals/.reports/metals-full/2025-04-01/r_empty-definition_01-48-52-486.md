error id: `<none>`.
file://<WORKSPACE>/data-cleaning-and-transformation-pipeline/build.sbt
empty definition using pc, found symbol in pc: `<none>`.
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 289
uri: file://<WORKSPACE>/data-cleaning-and-transformation-pipeline/build.sbt
text:
```scala
import Dependencies._

ThisBuild / scalaVersion     := "2.12.10"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "Data-Cleaning-and-Tra@@nsformation-Pipeline",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.1.1",
      "org.apache.spark" %% "spark-sql" % "3.1.1",
      munit % Test
    )
  )


```


#### Short summary: 

empty definition using pc, found symbol in pc: `<none>`.