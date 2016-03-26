package com.persist.dst

import org.apache.spark.sql.Column
import scala.language.experimental.macros
import scala.reflect.macros.whitebox.Context
import scala.reflect.runtime.universe._
import com.persist.dst.Columns._

object Transforms {

  private def getColumnInfo(c: Context)(t: c.universe.Type): Seq[(String, c.universe.Type)] = {
    val names = t.decls
      .filter { case d => d.isMethod && d.toString.startsWith("value ") }
      .map { case decl =>
        val ty = decl.asMethod.returnType
        val name = decl.name.toString()
        (name, ty)
      }
      .toSeq
    names
  }

  def Transform[TOLD, TNEW](f: (TOLD) => TNEW): Any = macro transformImpl[TOLD, TNEW]

  def transformImpl[Told: c.WeakTypeTag, Tnew: c.WeakTypeTag]
  (c: Context)(f: c.Expr[Any]): c.Expr[Any] = {
    import c.universe._
    val told = c.weakTypeTag[Told].tpe
    val tnew = c.weakTypeTag[Tnew].tpe
    val q =
      q"""
      (ds:Dataset[$told]) => {
        ds.rdd.map($f).toDF().as[$tnew]
      }
      """
    c.Expr(q)
  }

  def Join[TA, TB, TNEW]: Any = macro joinImpl[TA, TB, TNEW]

  def joinImpl[Ta: c.WeakTypeTag, Tb: c.WeakTypeTag, Tnew: c.WeakTypeTag](c: Context): c.Expr[Any] = {
    import c.universe._
    val ta = c.weakTypeTag[Ta].tpe
    val aNames = getColumnInfo(c)(ta)
    val tb = c.weakTypeTag[Tb].tpe
    val bNames = getColumnInfo(c)(ta)
    val tnew = c.weakTypeTag[Tnew].tpe
    val newNames = getColumnInfo(c)(tnew)
    val acolsExp = getColumns(c)(ta, "_1")
    val bcolsExp = getColumns(c)(tb, "_2")
    val fields = newNames map { case (name, ty) =>
      if (ty == c.weakTypeTag[Int]) {
        tq"IntColumn[ThisTransform]"
      } else if (ty == c.weakTypeOf[Boolean]) {
        tq"BooleanColumn[ThisTransform]"
      } else {
        tq"TyColumn[ThisTransform,$ty]"
      }
    }
    val qargs1 = for (((n, ty), i) <- newNames.zipWithIndex) yield {
      val pos = TermName(s"_${i + 1}")
      q"""(f.$pos.col).as($n)"""
    }
    // TODO use ThisTransformA in getColumns
    // TODO Distinguish A and B
    val q =
      q"""
         new {
             class ThisTransform extends Transform
             class ThisTransformA extends ThisTransform
             class ThisTransformB extends ThisTransform

             val acols = $acolsExp
             val bcols = $bcolsExp

             def map[T](akey: (acols.type) => TyColumn[ThisTransform,T],
             bkey: (bcols.type)=> TyColumn[ThisTransform,T],
             fields: (acols.type,bcols.type) => (..$fields)) = {
                val f = fields(acols,bcols)
                (dsa:Dataset[$ta],dsb:Dataset[$tb]) => {
                  dsa.as("_1").joinWith(dsb.as("_2"), akey(acols).col === bkey(bcols).col )
                  .toDF().select(..$qargs1).as[$tnew]
                 }
             }
             override def toString() = "Join[" + ${ta.toString} +
             "," + ${tb.toString} + "," + ${tnew.toString} + "]"
          }
      """
    c.Expr(q)
  }

  def Sort[T]: Any = macro sortImpl[T]

  def sortImpl[T: c.WeakTypeTag]
  (c: Context): c.Expr[Any] = {
    import c.universe._
    val t = c.weakTypeTag[T].tpe
    val names = getColumnInfo(c)(t)
    val colsExp = getColumns(c)(t, "")
    val q =
      q"""
         new {
             class ThisTransform extends Transform

             val cols = $colsExp

             def map(fields: (cols.type) => Seq[AnyColumn[ThisTransform]]) = {
                val f = fields(cols).map(_.col)
                (ds:Dataset[$t]) => {
                  ds.toDF().sort(f:_*).as[$t]
                 }
             }
             override def toString() = "Sort[" + ${t.toString} + "]"
          }
      """
    c.Expr(q)
  }

  def Select[TOLD, TNEW]: Any = macro selectImpl[TOLD, TNEW]

  def selectImpl[Told: c.WeakTypeTag, Tnew: c.WeakTypeTag]
  (c: Context): c.Expr[Any] = {
    import c.universe._
    val told = c.weakTypeTag[Told].tpe
    val oldNames = getColumnInfo(c)(told)
    val tnew = c.weakTypeTag[Tnew].tpe
    val newNames = getColumnInfo(c)(tnew)
    val colsExp = getColumns(c)(told, "")
    val fields = newNames map { case (name, ty) =>
      if (ty == c.weakTypeTag[Int]) {
        tq"IntColumn[ThisTransform]"
      } else if (ty == c.weakTypeOf[Boolean]) {
        tq"BooleanColumn[ThisTransform]"
      } else {
        tq"TyColumn[ThisTransform,$ty]"
      }
    }

    val qargs1 = for (((n, ty), i) <- newNames.zipWithIndex) yield {
      val pos = TermName(s"_${i + 1}")
      q"""(f.$pos.col).as($n)"""
    }
    val q =
      q"""
         new {
             class ThisTransform extends Transform

             val cols = $colsExp

             def map(fields: (cols.type) => (..$fields)) = {
                val f = fields(cols)
                (ds:Dataset[$told]) => {
                  ds.toDF().select(..$qargs1).as[$tnew]
                 }
             }
             override def toString() = "Select[" + ${told.toString} + "," + ${tnew.toString} + "]"
          }
      """
    c.Expr(q)
  }

  private def getColumns(c: Context)(ty: c.universe.Type, ext: String): c.Expr[Any] = {
    import c.universe._
    val transform = "ThisTransform" //+ ext
    val prefix = if (ext.isEmpty) "" else ext + "."
    val className = ty.toString
    val ci = getColumnInfo(c)(ty)
    val tytransform = TypeName(transform)
    val names = ci.flatMap { case (name, tyc) =>
      val colName = s"COLUMN_$name"
      val colNameN = TypeName(colName)
      val nameT = TermName(name)
      val ct = if (tyc == c.weakTypeOf[Int]) {
        tq"IntColumn[$tytransform]"
      } else if (tyc == c.weakTypeOf[Boolean]) {
        tq"BooleanColumn[$tytransform]"
      } else {
        tq"TyColumn[$tytransform,$tyc]"
      }
      val q1 =
        q"""
           class $colNameN extends $ct {
             val name = ${prefix + name}
             val col = new ColumnName(${prefix + name})
          }
        """
      val q2 =
        q"""
          val $nameT = new $colNameN
        """
      Seq(q1, q2)
    }
    val q1 = q"""new { ..$names }"""
    c.Expr(q1)
  }
}
