package io.phdata.snowpark.metrics

import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions._
import io.phdata.snowpark.helpers.ReformatOutputHelper

class DateDescriptionMetric extends UnivariateMetric {

   def runMetric(metricRunID: String, columnName: String, df: DataFrame): MetricResult = {

    val df_clean = df.filter(col(columnName).is_not_null)
    if (df_clean.count() > 0 && checkColumnTypeIsDate(columnName, df)) {

      val df_result = df_clean.agg(max(col(columnName)).as("date_max"),
        min(col(columnName)).as("date_min"),
        datediff("day", min(col(columnName)), max(col(columnName))).alias("date_range")
      )

      val stringResult = (new ReformatOutputHelper).convertDataframeToStringOutput(df_result)

      MetricResult(metricRunID, "Date Description Column - " + columnName, stringResult)

    }else {
      MetricResult(metricRunID, "Date Description Column - " + columnName, "Empty Table Or Null Column")
    }
  }

  def checkColumnTypeIsDate(columnName: String, df: DataFrame): Boolean = {
    val dateTypes = Seq("date", "datetime")
    dateTypes.contains(df.schema(columnName).dataType.typeName.toLowerCase)
  }
}
