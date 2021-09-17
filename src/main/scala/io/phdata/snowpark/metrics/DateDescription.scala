package io.phdata.snowpark.metrics

import com.snowflake.snowpark.{DataFrame, Window}
import com.snowflake.snowpark.functions.{builtin, col, current_timestamp, datediff, lit, max, min}
import com.snowflake.snowpark.types.DataTypes
import io.phdata.snowpark.helpers.TableName

class DateDescription(df: DataFrame) extends Metric {
  //TODO: validate columns of dataframe
  override val values: DataFrame = df
  override val tableSuffix: String = "date_description"

  override def latestValues: DataFrame = {
    val first = builtin("first_value")

    val ws = Window.partitionBy(
      col("database"),
      col("schema"),
      col("table"),
      col("column"),
    ).orderBy(col("timestamp").desc)

    values
      .select(
        first(col("database")).over(ws).as("database"),
        first(col("schema")).over(ws).as("schema"),
        first(col("table")).over(ws).as("table"),
        first(col("column")).over(ws).as("column"),
        first(col("timestamp")).over(ws).as("timestamp"),
        first(col("date_max")).over(ws).as("date_max"),
        first(col("date_min")).over(ws).as("date_min"),
        first(col("date_range")).over(ws).as("date_range"),
      )
  }
}

object DateDescription extends MetricObject {

  override def apply(tn: TableName, table: DataFrame): Option[DateDescription] = {
    val date_cols = table.schema.fields.filter(isDate).map(_.name)

    val values = date_cols.map(columnName => {
      val tc = col(columnName).cast(DataTypes.TimestampType)
      table.filter(tc.is_not_null)
        .agg(
          max(tc).as("date_max"),
          min(tc).as("date_min"),
        )
        .select(
          lit(tn.database).as("database"),
          lit(tn.schema).as("schema"),
          lit(tn.table).as("table"),
          lit(columnName).as("column"),
          current_timestamp().as("timestamp"),
          col("*"),
          datediff("day", col("date_min"), col("date_max")).as("date_range"),
        )
    }).reduceOption(_ union _)

    values match {
      case Some(df) => Some(new DateDescription(df))
      case None => None
    }
  }

}