name := "dst-demo"

organization := "com.persist"

version := "0.0.4"

scalaVersion := "2.11.8"

//scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-Ymacro-debug-lite")
scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature")

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.0.0"
)
