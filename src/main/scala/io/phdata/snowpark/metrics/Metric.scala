package io.phdata.snowpark.metrics

import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions._

trait Metric {
  val commonColumns = Seq("database", "schema", "table", "column", "timestamp")
  def values: DataFrame
  def tableSuffix: String

  def localColumns: Seq[String] = values.schema.map(_.name).filter(!commonColumns.contains(_))

  def union[T <: Metric](that: T): Metric = {
    this.getClass
      .getDeclaredConstructor(values.getClass)
      .newInstance(values.union(that.values))
      .asInstanceOf[T]
  }

  def getCSVHeader: String = {
    values.schema.fields.map(_.name).mkString("", ",", "\n")
  }

  def getCSVData: Seq[String] = {
    values.collect().map(r => r.toSeq.mkString("", ",", "\n"))
  }

  def getUnifiedDF: DataFrame = {
    values.select(
      Seq(lit(this.getClass.getName)) ++
      commonColumns.map(col) ++
      Seq(to_json(object_construct(localColumns.map(col):_*)).as("values"))
    )
  }
}
