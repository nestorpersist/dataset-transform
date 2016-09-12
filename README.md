## Strongly typed Scala operations for working with Spark Datasets

The addition of Datasets to Spark was a big step forward for Scala developers
by providing static typing rather than the dynamic typing of DataFrames.
Sadly, many operations on Datasets still require dynamic types where checking 
occurs at run-time.

The purpose of this project is to add a new set of transforms on Scala Datasets
that are fully static typing so checking of existance of field names and the types
of fields can occur entirely at compile-time.

The transforms operate on Datasets whose element type is a Scala case class. Each transform
takes one or more Datasets and transforms them to produce a new Dataset.

The current transforms are:

* **FuncMap** (implemented via RDD map operation).
* **FuncFilter** (implemented via Dataset filter operation).
* **SqlMap** (implemented via DataFrame select operation).
* **SqlFilter** (implemented via DataFrame filter operation).
* **SqlSort** (implemented via DataFrame sort operation).
* **SqlJoin** (implemented via Dataset joinWith and DataFrame select operations).
* **SqlAgg** (implemented via DataFrame groupBy and KeyedValueGroupedDataset agg operations)

Func operations are specified using Scala functions. Sql operations are specified using Spark Sql Columns.

For an example see

    demo/src/main/scala/com/persist/DstDemo.scala
    
To use code include

    "com.persist" % "dataset-transforms_2.11" % "0.0.5"

The implementation of these transforms makes use of Scala Whitebox macros.
These work fine in SBT and Eclipse but unfortunately are not fully supported in 
Intellij (the JetBrains people tell me that they have no plans to fully support 
Whitebox macros).

Work on this project was supported by 47 Degrees, a Lightbend and DataBricks partner
with strong expertise in functional programming, Scala and Spark.

[http://www.47deg.com/](http://www.47deg.com)
