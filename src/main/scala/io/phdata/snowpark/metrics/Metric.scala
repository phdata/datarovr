package io.phdata.snowpark.metrics

import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions._

trait Metric {
  val commonColumns = Seq("DATABASE", "SCHEMA", "TABLE", "COLUMN", "TIMESTAMP")
  def values: DataFrame
  def tableSuffix: String

  def getHTMLSnippet: String = "<h2>Not Implemented</h2>"

  def name: String = this.getClass.getName.split('.').last
  def localColumns: Seq[String] = values.schema.map(_.name).filter(!commonColumns.contains(_))

  def union[T <: Metric](that: T): Metric = {
    this.getClass
      .getDeclaredConstructor(values.getClass)
      .newInstance(values.union(that.values))
      .asInstanceOf[T]
  }

  def getCSV: Seq[String] = {
    val header = values.schema.fields.map(_.name).mkString("", ",", "\n")
    Seq(header) ++ values.collect().map(_.toSeq.mkString("", ",", "\n"))
  }

  def getUnifiedDF: DataFrame = {
    values.select(
      Seq(lit(name).as("metric")) ++
      commonColumns.map(col) ++
      Seq(to_json(
          object_construct(
            localColumns.flatMap(c => Seq(lit(c), col(c))):_*
          )
      ).as("values"))
    )
  }
}
