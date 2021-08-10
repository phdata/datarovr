package io.phdata.snowpark.metrics

import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions._
import io.phdata.snowpark.algorithms._
import io.phdata.snowpark.helpers.ReformatOutputHelper

class CorrelationMatrixMetric extends MultivariateMetric {

   override def runMetric(metricRunID: String, columnNames: List[String], df: DataFrame, tableName: String): MetricResult = {

     val numberTypes = Seq("integer", "long", "float", "double")

     val fields = df.schema.fields.filter(x => numberTypes.contains(x.dataType.typeName.toLowerCase)).map(y => y.name)

     if (fields.length > 0) {

       val columnNamesNum = columnNames.filter(x => fields.contains(x))

       val df_number = df.select(fields)

       if (columnNamesNum.size > 2 && df_number.count() > 0) {
          val accumulator = new Accumulator[String]
          val nonRepeatingCombPull = new NonRepeatingCombPull[String](columnNamesNum, 2)
          PermComb.constructPushGenerator(accumulator.push, nonRepeatingCombPull.iterator)

          val col_combinations = accumulator.listAccu.toArray

          val df_clean = df_number.na.drop(0, columnNamesNum)

          val df_result = df_clean.agg(col_combinations.map(x => {
              corr(col(x.head), col(x.last)).as(x.mkString("::"))
            }))

          val stringResult = (new ReformatOutputHelper).convertDataframeToStringOutput(df_result)

          MetricResult(metricRunID, "Correlation Matrix for - " + tableName, stringResult)
       }else {
          MetricResult(metricRunID, "Correlation Matrix for - " + tableName, "Empty Table/Not enough Number Columns")
       }
     }else {
       MetricResult(metricRunID, "Correlation Matrix for - " + tableName, "Empty Table/Not enough Number Columns")
     }

  }


}
