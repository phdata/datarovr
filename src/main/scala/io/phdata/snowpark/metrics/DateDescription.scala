package io.phdata.snowpark.metrics

import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions.{col, current_timestamp, datediff, lit, max, min}
import com.snowflake.snowpark.types.DataTypes
import io.phdata.snowpark.helpers.TableName

class DateDescription(df: DataFrame) extends Metric {
  //TODO: validate columns of dataframe
  override val values: DataFrame = df
  override val tableSuffix: String = "date_description"
}

object DateDescription extends MetricObject {

  def apply(tn: TableName, table: DataFrame): Option[DateDescription] = {
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