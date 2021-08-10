package io.phdata.snowpark.metrics

import com.snowflake.snowpark.DataFrame


trait UnivariateMetric extends Metric {

  def runMetric(metricRunID: String, columnName: String, df: DataFrame): MetricResult

}
