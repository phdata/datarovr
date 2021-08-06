package io.phdata.snowpark.metrics

import com.snowflake.snowpark.DataFrame

trait MultivariateMetric {

  def runMetric(metricRunID: String, columnNames: List[String], df: DataFrame): MetricResult
}
