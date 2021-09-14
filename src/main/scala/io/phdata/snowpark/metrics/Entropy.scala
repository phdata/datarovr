package io.phdata.snowpark.metrics

import com.snowflake.snowpark.{DataFrame, Window}
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types.DataTypes
import io.phdata.snowpark.helpers.TableName

class Entropy(df: DataFrame) extends Metric {
  //TODO: Validate columns of the dataframe
  override val values: DataFrame = df
  override val tableSuffix: String = "entropy"
}

object Entropy extends MetricObject {
  override def apply(tn: TableName, table: DataFrame): Option[Entropy] = {
    val numeric_cols = table.schema.fields.filter(isNumeric).map(_.name)
    val ws = Window.partitionBy(col("group"))

    val values = numeric_cols.map(columnName => {
      val dc = col(columnName).cast(DataTypes.DoubleType)
      table.filter(dc.is_not_null)
        .select(lit(1).as("group"), dc.as("value"))
        .withColumn("total", count(col("value")).over(ws))
        .groupBy(col("total"), col("value"))
        .count()
        .withColumn("probability", col("count")/col("total"))
        .withColumn("entropy", col("probability") * log(lit(2), col("probability")))
        .agg((sum(col("entropy"))*lit(-1)).as("entropy"))
        .select(
          lit(tn.database).as("database"),
          lit(tn.schema).as("schema"),
          lit(tn.table).as("table"),
          lit(columnName).as("column"),
          current_timestamp().as("timestamp"),
          col("entropy"),
        )
    }).reduceOption(_ union _)

    values match {
      case Some(df) => Some(new Entropy(df))
      case None => None
    }
  }
}
