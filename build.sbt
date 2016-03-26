name := "dst"

scalaVersion := "2.11.7"

lazy val transforms = Project("transforms", file("transforms")).settings()

lazy val demo = Project("demo", file("demo")).dependsOn("transforms").settings()


