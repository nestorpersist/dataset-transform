## Strongly typed Scala operations for working with Spark Datasets

The addition of DataSets to Spark was a big step forward for Scala developers
by providing statically typing rather than the dynamic typing of Dataframes.
Sadly, many operations on DataSets still require dynamic types where checking 
occurs at run-time.

The purpose of this project is to add a new set of transforms on Scala DataSets
that are fully statically typed.

The transforms operate on DataSets whose element type is a Scala case class. Each transform
takes one or more DataSets and transforms them to produce a new DataSet.

The current transforms are:

* **Transform** (implemented via RDD map operation).
* **Select** (implemented via Dataframe select operation).
* **Sort** (implemented via Dataframe sort operation).
* **Join** (implemented via DataSet joinWith and Dataframe select operations).

For an example see

    demo/src/main/scala/com/persist/DstDemo.scala
    
To use code include

    "com.persist" % "dataset-transforms_2.11" % "0.0.1"

The implementation of these transforms makes use of Scala Whitebox macros.
These work fine in SBT and Eclipse but unfortunately are not fully supported in 
Intellij (the JetBrains people tell me that they have no plans to fully support 
Whitebox macros).

Work on this project was supported by 47 Degrees, a Lightbend and DataBricks partner
with strong expertise in functional programming, Scala and Spark.

[http://www.47deg.com/](http://www.47deg.com)
