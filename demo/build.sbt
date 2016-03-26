name := "persist-tf-demo"

organization := "com.persist"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.7"

//scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-Ymacro-debug-lite")
scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature")


//viewSettings

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.6.1",
  //"org.apache.spark" % "spark-mllib_2.11" % "1.6.0",
  //"org.apache.spark" % "spark-graphx_2.11" % "1.6.0",
  "org.apache.spark" % "spark-sql_2.11" % "1.6.1"
  //"com.chuusai" %% "shapeless" % "2.3.0",
  //"org.specs2" %% "specs2-core" % "3.6.4" % "test"
)
