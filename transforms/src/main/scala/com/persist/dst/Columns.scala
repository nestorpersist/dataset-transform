package com.persist.dst

import org.apache.spark.sql.Column
import scala.reflect.runtime.universe._

object Columns {

  // TODO desc only available for sort
  // TODO fix problem with select has only a single value
  // TODO add more operations
  // TODO allow ops other than desc on sort???

  abstract class Transform

  private def typeTag[T](implicit tag: WeakTypeTag[T]) = {
    tag
  }


  abstract class AnyColumn[TRANSFORM <: Transform] {
    val name: String
    val col: Column

    private def c = col

    private def n = name

    def desc = new AnyColumn[TRANSFORM] {
      val col = c.desc
      val name = n + ".desc"
    }

    override def toString() = {
      s"Column($name)"
    }
  }

  abstract class TyColumn[TRANSFORM <: Transform, TCOL: TypeTag]
    extends AnyColumn[TRANSFORM] {

    def ===(other: TyColumn[TRANSFORM, TCOL]) = {
      val c = col
      val n = name
      new BooleanColumn[TRANSFORM] {
        val name = s"n == ${other.name}"
        val col = c === other.col
      }
    }

    override def toString() =
      s"Column($name:${typeTag[TCOL].tpe.toString})"
  }

  abstract class IntColumn[TRANSFORM <: Transform] extends TyColumn[TRANSFORM, Int] {

    def +(i: Int) = {
      val n = name
      val c = col
      new IntColumn[TRANSFORM] {
        val name = s"$n + $i"
        val col = c + i
      }
    }

    def +(other: IntColumn[TRANSFORM]) = {
      val c = col
      val n = name
      new IntColumn[TRANSFORM] {
        val name = s"n + ${other.name}"
        val col = c + other.col
      }
    }

    def *(i: Int) = {
      val n = name
      val c = col
      new IntColumn[TRANSFORM] {
        val name = s"$n * $i"
        val col = c * i
      }
    }
  }

  abstract class BooleanColumn[TRANSFORM <: Transform] extends TyColumn[TRANSFORM, Boolean] {
  }

}
