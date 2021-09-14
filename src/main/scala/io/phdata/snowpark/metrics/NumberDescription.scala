package io.phdata.snowpark.metrics
import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types.DataTypes
import io.phdata.snowpark.helpers.TableName

class NumberDescription(df: DataFrame) extends Metric {
  //TODO: validate columns of dataframe
  override val values: DataFrame = df
  override val tableSuffix: String = "number_description"
}

object NumberDescription extends MetricObject {
  override def apply(tn: TableName, table: DataFrame): Option[NumberDescription] = {
    val numeric_cols = table.schema.fields.filter(isNumeric).map(_.name)

    val values = numeric_cols.map(columnName => {
      val dc = col(columnName).cast(DataTypes.DoubleType)
      table.filter(dc.is_not_null).agg(
        stddev(dc).alias("stddev"),
        mean(dc).alias("mean"),
        max(dc).as("max"),
        min(dc).as("min"),
        variance(dc).as("variance"),
        when(count(dc) <= count_distinct(dc)*2, kurtosis(dc))
          .otherwise(lit(null).cast(DataTypes.DoubleType)).as("kurtosis"),
        when(count(dc) <= count_distinct(dc)*2, skew(dc))
          .otherwise(lit(null).cast(DataTypes.DoubleType)).as("skewness"),
        approx_percentile(dc, 0.25).as("percentile25"),
        approx_percentile(dc, 0.5).as("percentile50"),
        approx_percentile(dc, 0.75).as("percentile75"),
        (approx_percentile(dc, 0.75) - approx_percentile(col(columnName), 0.25)).as("iqr"),
        when(mean(dc) > 0, stddev(dc) / mean(dc))
          .otherwise(lit(null)).as("cv"),
        sum(dc).as("sum"),
        count(dc).as("count"),
      ).select(
        lit(tn.database).as("database"),
        lit(tn.schema).as("schema"),
        lit(tn.table).as("table"),
        lit(columnName).as("column"),
        current_timestamp().as("timestamp"),
        col("*"),
      )
    }).reduceOption(_ union _)

    values match {
      case Some(df) => Some(new NumberDescription(df))
      case None => None
    }
  }

}