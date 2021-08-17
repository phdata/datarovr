package io.phdata.snowpark.helpers

import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions.{col, object_construct, to_json}

class ReformatOutputHelper {

  def convertDataframeToStringOutput(df: DataFrame): String ={
    "["+df.withColumn("value", to_json(object_construct(col("*"))))
      .select("value").collect().map(_.getString(0)).mkString(",")+"]"
  }

}
