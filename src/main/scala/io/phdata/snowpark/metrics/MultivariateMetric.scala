package io.phdata.snowpark.metrics

import com.snowflake.snowpark.DataFrame

trait MultivariateMetric extends Metric {

  def runMetric(metricRunID: String, columnNames: List[String], df: DataFrame, tableName: String): MetricResult
}
