package com.persist.dst

import org.apache.spark.sql.Column
import scala.reflect.runtime.universe._

object DstColumns {

  // TODO desc only available for sort
  // TODO add more operations
  // TODO allow ops other than desc on sort???

  abstract class DstTransform

  private def typeTag[T](implicit tag: WeakTypeTag[T]) = {
    tag
  }


  abstract class DstColumn[TRANSFORM <: DstTransform] {
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

  abstract class DstTypedColumn[TRANSFORM <: DstTransform, TCOL: TypeTag]
    extends DstColumn[TRANSFORM] {

    def ===(other: DstTypedColumn[TRANSFORM, TCOL]) = {
      val c = col
      val n = name
      new DstBooleanColumn[TRANSFORM] {
        val name = s"n == ${other.name}"
        val col = c === other.col
      }
    }

    override def toString() =
      s"Column($name:${typeTag[TCOL].tpe.toString})"
  }

  abstract class DstIntColumn[TRANSFORM <: DstTransform] extends DstTypedColumn[TRANSFORM, Int] {

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
  }

  abstract class DstBooleanColumn[TRANSFORM <: DstTransform] extends DstTypedColumn[TRANSFORM, Boolean] {
  }

}
