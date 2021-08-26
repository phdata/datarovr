package io.phdata.snowpark.metrics

import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions.{col, current_timestamp, lit, udf}
import io.phdata.snowpark.helpers.TableName

class MaskProfile(df: DataFrame) extends Metric {
  //TODO: validate columns of dataframe
  override val values: DataFrame = df
  override val tableSuffix: String = "mask_profile_metric"
}

object MaskProfile extends MetricObject {
  def apply(tn: TableName, table: DataFrame): Option[MaskProfile] = {
    val regex_digit = "[0-9]".r
    val regex_char = "[a-zA-Z]".r
    val maskUdf = udf((s: String) => regex_digit.replaceAllIn(regex_char.replaceAllIn(s, "X"), "N"))

    //Snowflake crashes when attempting to process an empty dataframe
    if (table.count() == 0) {
      return None
    }

    val columns = table.schema.fields.map(_.name)
    val values = columns.map(columnName => {
      table.filter(col(columnName).is_not_null)
        .select(maskUdf(col(columnName)).as("mask"))
        .groupBy("mask")
        .count()
        .select(
          lit(tn.database).as("database"),
          lit(tn.schema).as("schema"),
          lit(tn.table).as("table"),
          lit(columnName).as("column"),
          current_timestamp().as("timestamp"),
          col("mask"),
          col("count"),
        )
    }).reduceOption(_ union _)

    values match {
      case Some(df) => Some(new MaskProfile(df))
      case None => None
    }
  }

}