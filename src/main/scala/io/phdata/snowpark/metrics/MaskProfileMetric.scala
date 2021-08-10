package io.phdata.snowpark.metrics

import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions._
import io.phdata.snowpark.helpers.ReformatOutputHelper

class MaskProfileMetric extends UnivariateMetric {

  /**
   * Will generate a mask of the column and return the coount of records with the same mask.
   * @param metricRunID Common idea to tie all metrics together for a run.
   * @param columnName Column that should have the metrics calculated.
   * @param df Dataframe holding all of the values
   * @return returns a metric result that will have the metrics in it.
   */
  override def runMetric(metricRunID: String, columnName: String, df: DataFrame): MetricResult = {
    val df_filtered = df.filter(col(columnName).is_not_null)
    if (df_filtered.count() > 0) {
      val regex_digit = "[0-9]".r
      val regex_char = "[a-zA-Z]".r
      val maskUdf = udf((s: String) => regex_digit.replaceAllIn(regex_char.replaceAllIn(s, "X"), "N"))

      val df_string = df_filtered.withColumn(columnName, maskUdf(df(columnName)))

      val df_result = df_string.groupBy(columnName).count()

      val stringResult = (new ReformatOutputHelper).convertDataframeToStringOutput(df_result)

      MetricResult(metricRunID, "Mask Profile Column - " + columnName, stringResult)
    }
    else {
      MetricResult(metricRunID, "Mask Profile Column - " + columnName, "Empty Table Or Null Column")
    }
  }
}
