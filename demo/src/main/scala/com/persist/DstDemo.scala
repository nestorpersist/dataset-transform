package com.persist

import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

import scala.language.reflectiveCalls
import com.persist.dst.DstTransforms._
import com.persist.dst.DstColumns._
import org.apache.spark.sql.types.IntegerType

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
    val rdd = sc.parallelize(abcs)
    val dsABC = rdd.toDF().as[ABC]
    debug("ABC", dsABC)

    val ca1 = CA("THREE", 3)
    val ca2 = CA("FIVE", 5)
    val ca3 = CA("TEN", 10)
    val cas = Seq(ca1, ca2, ca3)
    val rdd1 = sc.parallelize(cas)
    val dsCA = rdd1.toDF().as[CA]
    debug("CA", dsCA)


    val smap = SqlMap[ABC, CA].act(cols => (cols.b, cols.a * 2 + cols.a))
    val ds1 = smap(dsABC)
    debug("SMAP", ds1)

    val fmap = FuncMap((x: ABC) => CA(x.c, x.a))
    val ds2 = fmap(dsABC)
    debug("FMAP", ds2)

    val smap1 = SqlMap[ABC, BOOL].act(cols => (cols.b === cols.c, cols.a))
    val ds3 = smap1(dsABC)
    debug("SMAP1", ds3)

    val sort1 = SqlSort[ABC].act(cols => Seq(cols.b.desc))
    val ds4 = sort1(dsABC)
    debug("SSORT", ds4)

    val join = SqlJoin[ABC, CA, ABC].act(_.a, _.a, (abc, ca) => (abc.a, abc.b, ca.c))
    val ds5 = join(dsABC, dsCA)
    debug("SJOIN", ds5)

    val ffilter = FuncFilter((x: ABC) => x.a > 4)
    val ds6 = ffilter(dsABC)
    debug("FFILTER", ds6)

    val sfilter = SqlFilter[ABC].act(cols => cols.a === 3 || cols.a === 5)
    val ds7 = sfilter(dsABC)
    debug("SFILTER", ds7)

    sc.stop()
  }


  class GroupDemo {
    def debug[T](name: String, ds: Dataset[T]) = println(s"$name: ${ds.rdd.collect.toList}")

    val conf = new SparkConf().setMaster(s"local[*]").setAppName("test").set("spark.app.id", "id")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    import sqlc.implicits._

    val ca10 = CA("A", 3)
    val ca11 = CA("B", 2)
    val ca12 = CA("A", 7)
    val ca13 = CA("B", 23)

    val cas1 = Seq(ca10, ca11, ca12, ca13)
    val dsCA1 = sc.parallelize(cas1).toDF().as[CA]

    val agg = SqlAgg[CA, CA].act(cols => (cols.c, cols.a.sum))
    val ds8 = agg(dsCA1)
    debug("AGG", ds8)

    sc.stop
  }

  def main(args: Array[String]): Unit = {
    val s = new Demo
    val g = new GroupDemo
  }

}
