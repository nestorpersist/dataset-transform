package com.persist.dst

import org.apache.spark.sql.Column
import scala.reflect.runtime.universe._

object DstColumns {

  // TODO desc only available for sort
  // TODO add String column type (with compare ops)
  // TODO allow ops other than desc on sort???

  abstract class DstTransform

  private def typeTag[T](implicit tag: WeakTypeTag[T]) = {
    tag
  }


  trait DstColumn[TRANSFORM <: DstTransform] {
    val name: String
    val col: Column

    private def c = col

    private def n = name

    def desc = new DstColumn[TRANSFORM] {
      val col = c.desc
      val name = n + ".desc"
    }

    override def toString() = {
      s"Column($name)"
    }
  }

  abstract class AggTypedColumn[TRANSFORM <: DstTransform, TCOL:TypeTag]

  abstract class DstTypedColumn[TRANSFORM <: DstTransform, TCOL: TypeTag]
    extends AggTypedColumn[TRANSFORM, TCOL] with DstColumn[TRANSFORM]  {

    def ===(other: DstTypedColumn[TRANSFORM, TCOL]) = {
      val c = col
      val n = name
      new DstBooleanColumn[TRANSFORM] {
        val name = s"n == ${other.name}"
        val col = c === other.col
      }
    }

    def !==(other: DstTypedColumn[TRANSFORM, TCOL]) = {
      val c = col
      val n = name
      new DstBooleanColumn[TRANSFORM] {
        val name = s"n != ${other.name}"
        val col = c !== other.col
      }
    }

    def <(other: DstIntColumn[TRANSFORM]) = {
      val c = col
      val n = name
      new DstBooleanColumn[TRANSFORM] {
        val name = s"n < ${other.name}"
        val col = c < other.col
      }
    }

    def >(other: DstIntColumn[TRANSFORM]) = {
      val c = col
      val n = name
      new DstBooleanColumn[TRANSFORM] {
        val name = s"n > ${other.name}"
        val col = c > other.col
      }
    }

    def <=(other: DstIntColumn[TRANSFORM]) = {
      val c = col
      val n = name
      new DstBooleanColumn[TRANSFORM] {
        val name = s"n <= ${other.name}"
        val col = c <= other.col
      }
    }

    def >=(other: DstIntColumn[TRANSFORM]) = {
      val c = col
      val n = name
      new DstBooleanColumn[TRANSFORM] {
        val name = s"n >= ${other.name}"
        val col = c >= other.col
      }
    }

    override def toString() =
      s"Column($name:${typeTag[TCOL].tpe.toString})"
  }

    abstract class DstIntColumn[TRANSFORM <: DstTransform] extends DstTypedColumn[TRANSFORM, Int]  {

    def ===(i: Int) = {
      val n = name
      val c = col
      new DstBooleanColumn[TRANSFORM] {
        val name = s"$n === $i"
        val col = c === i
      }
    }

    def !==(i: Int) = {
      val n = name
      val c = col
      new DstBooleanColumn[TRANSFORM] {
        val name = s"$n !== $i"
        val col = c !== i
      }
    }

    def +(i: Int) = {
      val n = name
      val c = col
      new DstIntColumn[TRANSFORM] {
        val name = s"$n + $i"
        val col = c + i
      }
    }

    def +(other: DstIntColumn[TRANSFORM]) = {
      val c = col
      val n = name
      new DstIntColumn[TRANSFORM] {
        val name = s"n + ${other.name}"
        val col = c + other.col
      }
    }

    def *(i: Int) = {
      val n = name
      val c = col
      new DstIntColumn[TRANSFORM] {
        val name = s"$n * $i"
        val col = c * i
      }
    }

    def *(other: DstIntColumn[TRANSFORM]) = {
      val c = col
      val n = name
      new DstIntColumn[TRANSFORM] {
        val name = s"n * ${other.name}"
        val col = c * other.col
      }
    }

    def <(i: Int) = {
      val n = name
      val c = col
      new DstBooleanColumn[TRANSFORM] {
        val name = s"$n < $i"
        val col = c < i
      }
    }

    def >(i: Int) = {
      val n = name
      val c = col
      new DstBooleanColumn[TRANSFORM] {
        val name = s"$n > $i"
        val col = c > i
      }
    }

    def <=(i: Int) = {
      val n = name
      val c = col
      new DstBooleanColumn[TRANSFORM] {
        val name = s"$n <= $i"
        val col = c <= i
      }
    }

    def >=(i: Int) = {
      val n = name
      val c = col
      new DstBooleanColumn[TRANSFORM] {
        val name = s"$n >= $i"
        val col = c >= i
      }
    }

    def sum = {
      new AggIntColumn[TRANSFORM](this, "sum")
    }

    def count = {
      new AggIntColumn[TRANSFORM](this, "count")
    }

    def max = {
      new AggIntColumn[TRANSFORM](this, "max")
    }
  }

  class AggIntColumn[TRANSFORM <: DstTransform](val col:DstIntColumn[TRANSFORM], val kind:String) extends AggTypedColumn[TRANSFORM, Int] {
    override def toString() = s"$col.$kind"
  }

  abstract class DstBooleanColumn[TRANSFORM <: DstTransform] extends DstTypedColumn[TRANSFORM, Boolean] {

    def &&(b: Boolean) = {
      val n = name
      val c = col
      new DstBooleanColumn[TRANSFORM] {
        val name = s"$n && $b"
        val col = c && b
      }
    }

    def &&(other: DstBooleanColumn[TRANSFORM]) = {
      val c = col
      val n = name
      new DstBooleanColumn[TRANSFORM] {
        val name = s"n && ${other.name}"
        val col = c && other.col
      }
    }

    def ||(b: Boolean) = {
      val n = name
      val c = col
      new DstBooleanColumn[TRANSFORM] {
        val name = s"$n || $b"
        val col = c || b
      }
    }

    def ||(other: DstBooleanColumn[TRANSFORM]) = {
      val c = col
      val n = name
      new DstBooleanColumn[TRANSFORM] {
        val name = s"n || ${other.name}"
        val col = c || other.col
      }
    }

    def !() = {
      val c = col
      val n = name
      new DstBooleanColumn[TRANSFORM] {
        val name = s"! n"
        val col = ! c
      }
    }
  }
}
