import Dependencies.*

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organizationName := "example"

lazy val akkaVersion = "2.7.0"
lazy val alpakkaKafkaVersion = "4.0.2"
lazy val circeVersion = "0.14.1"

lazy val root = (project in file("."))
  .settings(
    name := "producer",
    Compile / mainClass := Some("example.ProducerDemo"),

    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream-kafka" % alpakkaKafkaVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "org.apache.kafka" % "kafka-clients" % "3.2.3",
      "ch.qos.logback" % "logback-classic" % "1.2.10",
      "org.scalatest" %% "scalatest" % "3.2.15" % Test,
      munit % Test
    )
  )