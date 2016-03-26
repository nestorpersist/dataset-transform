name := "dst-demo"

organization := "com.persist"

version := "0.0.1"

scalaVersion := "2.11.7"

//scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-Ymacro-debug-lite")
scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature")

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.6.1",
  "org.apache.spark" % "spark-sql_2.11" % "1.6.1"
)
