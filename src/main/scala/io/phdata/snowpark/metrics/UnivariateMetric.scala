package io.phdata.snowpark.metrics

import com.snowflake.snowpark.DataFrame

@SerialVersionUID(123453L)
trait UnivariateMetric extends Serializable {

  def runMetric(metricRunID: String, columnName: String, df: DataFrame): MetricResult

}
