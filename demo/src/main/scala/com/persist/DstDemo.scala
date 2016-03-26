package com.persist

import org.apache.spark.sql._
import org.apache.spark.{SparkContext, SparkConf}
import scala.language.reflectiveCalls
import com.persist.dst.DstTransforms._
import com.persist.dst.DstColumns._

case class ABC(a: Int, b: String, c: String)

case class CA(c: String, a: Int)

case class BOOL(b: Boolean, i: Int)

object DstDemo {


  class Demo {
    def debug[T](name: String, ds: Dataset[T]) = println(s"$name: ${ds.rdd.collect.toList}")

    val conf = new SparkConf().setMaster(s"local[*]").setAppName("test").set("spark.app.id", "id")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    import sqlc.implicits._

    val abc = ABC(3, "foo", "test")
    val abc1 = ABC(5, "xxx", "alpha")
    val abc3 = ABC(10, "aaa", "aaa")
    val abcs = Seq(abc, abc1, abc3)

    val ca1 = CA("THREE", 3)
    val ca2 = CA("FIVE", 5)
    val ca3 = CA("TEN", 10)
    val cas = Seq(ca1, ca2, ca3)
    val rdd1 = sc.parallelize(cas)
    val dsca = rdd1.toDF().as[CA]

    val rdd = sc.parallelize(abcs)
    val ds = rdd.toDF().as[ABC]
    debug("ABC", ds)

    val select = Select[ABC, CA].map(cols => (cols.b, cols.a * 2 + cols.a))
    val ds1 = select(ds)
    debug("SELECT", ds1)

    val transform = Transform((x: ABC) => CA(x.c, x.a))
    val ds2 = transform(ds)
    debug("TRANSFORM", ds2)

    val select1 = Select[ABC, BOOL].map(cols => (cols.b === cols.c, cols.a))
    val ds3 = select1(ds)
    debug("SELECT1", ds3)

    val sort1 = Sort[ABC].map(cols => Seq(cols.b.desc))
    val ds4 = sort1(ds)
    debug("SORT", ds4)

    val join = Join[ABC, CA, ABC].map(_.a, _.a, (abc, ca) => (abc.a, abc.b, ca.c))
    val ds5 = join(ds, dsca)
    debug("JOIN", ds5)

    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    val s = new Demo
  }

}
