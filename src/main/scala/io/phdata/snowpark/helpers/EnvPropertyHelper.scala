package io.phdata.snowpark.helpers

class EnvPropertyHelper {

  val snowflakePropertyNames: Seq[String] = Seq("SNOWFLAKE_URL", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_FILE", "SNOWFLAKE_DB",
      "SNOWFLAKE_SCHEMA", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_PRIVATEKEY", "SNOWFLAKE_ROLE", "SNOWFLAKE_PRIVATE_KEY_FILE_PWD")

  val applicationPropertyNames: Seq[String] = Seq("SCHEMA", "METRIC_TABLE", "UNIVARIATE_TESTS", "MULTIVARIATE_TESTS")

  def getSnowflakeConnectionProperties(): Map[String, String] ={
     snowflakePropertyNames.map(x => (x.replace("SNOWFLAKE_", ""), sys.env.getOrElse(x, ""))).toMap.filter(_._2 != "")
  }

  def getApplicationProperties(): Map[String, String] = {
    applicationPropertyNames.map(x => (x, sys.env.getOrElse(x, ""))).toMap.filter(_._2 != "")
  }

}
