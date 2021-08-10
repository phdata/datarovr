package io.phdata.snowpark.metrics
import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types.DataTypes
import io.phdata.snowpark.algorithms.ShannonEntropy
import io.phdata.snowpark.helpers.ReformatOutputHelper

class NumberDescriptionMetric extends UnivariateMetric {

   def runMetric(metricRunID: String, columnName: String, df: DataFrame): MetricResult = {

    val df_filtered = df.filter(col(columnName).is_not_null)
    if (df_filtered.count() > 0 && checkColumnTypeIsNumeric(columnName, df)) {
      val df_clean = df_filtered

      val df_result = df_clean.agg(stddev(col(columnName)).alias("stddev"),
        mean(col(columnName)).alias("mean"), max(col(columnName)).as("max"),
        min(col(columnName)).as("min"), variance(col(columnName)).as("variance"),
        kurtosis(col(columnName).cast(DataTypes.DoubleType)).as("kurtosis"),
        skew(col(columnName)).as("skewness"),
        approx_percentile(col(columnName), 0.25).as("percentile25"),
        approx_percentile(col(columnName), 0.5).as("percentile50"),
        approx_percentile(col(columnName), 0.75).as("percentile75"),
        (approx_percentile(col(columnName), 0.75) - approx_percentile(col(columnName), 0.25)).as("iqr"),
        (stddev(col(columnName)) / mean(col(columnName))).as("cv"),
        sum(col(columnName)).as("sum"), count(col(columnName)).as("count"),
        {
          def entropyUdf = udf((values: Array[String]) => {
            ShannonEntropy.entropy(values.map(_.toDouble.ceil)).toString
          })
          entropyUdf(array_agg(col(columnName))).as("entropy")
        }
      )

      val stringResult = (new ReformatOutputHelper).convertDataframeToStringOutput(df_result)

      MetricResult(metricRunID, "Number Description Column - " + columnName, stringResult)

    }else {
      MetricResult(metricRunID, "Number Description Column - " + columnName, "Empty Table Or Null Column")
    }
  }

  def checkColumnTypeIsNumeric(columnName: String, df: DataFrame): Boolean = {
    val numberTypes = Seq("integer", "long", "float", "double")
    numberTypes.contains(df.schema(columnName).dataType.typeName.toLowerCase)
  }
}
