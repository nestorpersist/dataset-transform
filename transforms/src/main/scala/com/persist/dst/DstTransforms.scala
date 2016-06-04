package com.persist.dst

import com.persist.dst.DstColumns.{AggIntColumn, DstTransform}
import org.apache.spark.sql.DataFrame

import scala.language.experimental.macros
import scala.reflect.macros.whitebox.Context
//import scala.reflect.runtime.universe._
//import com.persist.dst.DstColumns._

object DstTransforms {
  // TODO fix problem with select has only a single value

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

  def FuncMap[TOLD, TNEW](f: (TOLD) => TNEW): Any = macro funcMapImpl[TOLD, TNEW]

  def funcMapImpl[Told: c.WeakTypeTag, Tnew: c.WeakTypeTag]
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

  def SqlJoin[TA, TB, TNEW]: Any = macro SqlJoinImpl[TA, TB, TNEW]

  def SqlJoinImpl[Ta: c.WeakTypeTag, Tb: c.WeakTypeTag, Tnew: c.WeakTypeTag](c: Context): c.Expr[Any] = {
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
      tq"DstTypedColumn[ThisTransform,$ty]"
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
             class ThisTransform extends DstTransform
             class ThisTransformA extends ThisTransform
             class ThisTransformB extends ThisTransform

             val acols = $acolsExp
             val bcols = $bcolsExp

             def act[T](akey: (acols.type) => DstTypedColumn[ThisTransform,T],
             bkey: (bcols.type)=> DstTypedColumn[ThisTransform,T],
             fields: (acols.type,bcols.type) => (..$fields)) = {
                val f = fields(acols,bcols)
                (dsa:Dataset[$ta],dsb:Dataset[$tb]) => {
                  dsa.as("_1").joinWith(dsb.as("_2"), akey(acols).col === bkey(bcols).col )
                  .toDF().select(..$qargs1).as[$tnew]
                 }
             }
             override def toString() = "SqlJoin[" + ${ta.toString} +
             "," + ${tb.toString} + "," + ${tnew.toString} + "]"
          }
      """
    c.Expr(q)
  }

  def SqlSort[T]: Any = macro SqlSortImpl[T]

  def SqlSortImpl[T: c.WeakTypeTag]
  (c: Context): c.Expr[Any] = {
    import c.universe._
    val t = c.weakTypeTag[T].tpe
    val names = getColumnInfo(c)(t)
    val colsExp = getColumns(c)(t, "")
    val q =
      q"""
         new {
             class ThisTransform extends DstTransform

             val cols = $colsExp

             def act(fields: (cols.type) => Seq[DstColumn[ThisTransform]]) = {
                val f = fields(cols).map(_.col)
                (ds:Dataset[$t]) => {
                  ds.toDF().sort(f:_*).as[$t]
                 }
             }
             override def toString() = "SqlSort[" + ${t.toString} + "]"
          }
      """
    c.Expr(q)
  }

  def SqlMap[TOLD, TNEW]: Any = macro sqlMapImpl[TOLD, TNEW]

  def sqlMapImpl[Told: c.WeakTypeTag, Tnew: c.WeakTypeTag]
  (c: Context): c.Expr[Any] = {
    import c.universe._
    val told = c.weakTypeTag[Told].tpe
    val oldNames = getColumnInfo(c)(told)
    val tnew = c.weakTypeTag[Tnew].tpe
    val newNames = getColumnInfo(c)(tnew)
    val colsExp = getColumns(c)(told, "")
    val fields = newNames map { case (name, ty) =>
      tq"DstTypedColumn[ThisTransform,$ty]"
    }

    val qargs1 = for (((n, ty), i) <- newNames.zipWithIndex) yield {
      val pos = TermName(s"_${i + 1}")
      q"""(f.$pos.col).as($n)"""
    }
    val q =
      q"""
         new {
             class ThisTransform extends DstTransform

             val cols = $colsExp

             def act(fields: (cols.type) => (..$fields)) = {
                val f = fields(cols)
                (ds:Dataset[$told]) => {
                  ds.toDF().select(..$qargs1).as[$tnew]
                 }
             }
             override def toString() = "SqlMap[" + ${told.toString} + "," + ${tnew.toString} + "]"
          }
      """
    c.Expr(q)
  }


  def SqlAgg[TOLD, TNEW]: Any = macro sqlAggImpl[TOLD, TNEW]

  def sqlAggImpl[Told: c.WeakTypeTag, Tnew: c.WeakTypeTag]
  (c: Context): c.Expr[Any] = {
    import c.universe._
    val told = c.weakTypeTag[Told].tpe
    val oldNames = getColumnInfo(c)(told)
    val tnew = c.weakTypeTag[Tnew].tpe
    val newNames = getColumnInfo(c)(tnew)
    val colsExp = getColumns(c)(told, "")
    val fields = newNames map { case (name, ty) =>
      if (ty.toString() == "Int") {
        tq"AggTypedColumn[ThisTransform,$ty]"
      } else {
        tq"DstTypedColumn[ThisTransform,$ty]"
      }
    }

    val qargs1 = for (((n, ty), i) <- newNames.zipWithIndex) yield {
      val pos = TermName(s"_${i + 1}")
      q"""(f.$pos.col).as($n)"""
    }

    val q =
      q"""
         new {
             class ThisTransform extends DstTransform

             def fix1(df:DataFrame, a:AggIntColumn[ThisTransform]):DataFrame = {
               df.withColumnRenamed(a.kind ++ "(" ++  a.col.name ++ ")", a.col.name)
               .withColumn(a.col.name, a.col.col.cast(IntegerType))
             }

             def fixAll(df:DataFrame, as:Seq[AggIntColumn[ThisTransform]]) = as.foldLeft(df)(fix1)

             val cols = $colsExp

             def act(fields: (cols.type) => (..$fields)) = {
                val s = fields(cols).productIterator.toList
                val f = s.collect{case x:DstColumn[ThisTransform] @unchecked => x}.map(_.col)
                val a = s.collect{case x:AggIntColumn[ThisTransform] @unchecked => x}
                val m:Map[String,String] = a.map{case aic => aic.col.name -> aic.kind}.toMap
                (ds:Dataset[$told]) => {
                  val df1 = ds.toDF().groupBy(f:_*).agg(m)
                  fixAll(df1,a).as[$tnew]
                 }
             }
             override def toString() = "SqlAgg[" + ${told.toString} + "," + ${tnew.toString} + "]"
          }
      """
    c.Expr(q)
  }




  def FuncFilter[TOLD](f: (TOLD) => Boolean): Any = macro funcFilterImpl[TOLD]

  def funcFilterImpl[Told: c.WeakTypeTag]
  (c: Context)(f: c.Expr[Any]): c.Expr[Any] = {
    import c.universe._
    val told = c.weakTypeTag[Told].tpe
    val q =
      q"""
      (ds:Dataset[$told]) => {
        ds.filter($f)
      }
      """
    c.Expr(q)
  }

  def SqlFilter[TOLD]: Any = macro sqlFilterImpl[TOLD]

  def sqlFilterImpl[Told: c.WeakTypeTag]
  (c: Context): c.Expr[Any] = {
    import c.universe._
    val told = c.weakTypeTag[Told].tpe
    val oldNames = getColumnInfo(c)(told)
    val colsExp = getColumns(c)(told, "")
    val q =
      q"""
         new {
             class ThisTransform extends DstTransform

             val cols = $colsExp

             def act(fields: (cols.type) => DstBooleanColumn[ThisTransform]) = {
                val f = fields(cols)
                (ds:Dataset[$told]) => {
                  ds.toDF().filter(f.col).as[$told]
                 }
             }
             override def toString() = "SqlFilter[" + ${told.toString} + "]"
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
        tq"DstIntColumn[$tytransform]"
      } else if (tyc == c.weakTypeOf[Boolean]) {
        tq"DstBooleanColumn[$tytransform]"
      } else {
        tq"DstTypedColumn[$tytransform,$tyc]"
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
