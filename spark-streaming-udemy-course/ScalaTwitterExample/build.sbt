name := "TwitterExample"

version := "0.1"

scalaVersion := "2.11.0"


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.1",
  "org.apache.spark" % "spark-streaming_2.11" % "2.2.1")
