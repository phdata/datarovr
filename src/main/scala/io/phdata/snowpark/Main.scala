package io.phdata.snowpark

import com.snowflake.snowpark.functions.col
import com.snowflake.snowpark.types.{StringType, StructField, StructType}
import com.snowflake.snowpark.{Row, SaveMode, Session}
import io.phdata.snowpark.helpers.YamlPropertiesHelper
import io.phdata.snowpark.metrics._

object Main {

  def main(args: Array[String]) {

    val session = Session.builder.configFile("src/main/scala/io/phdata/snowpark/resources/snowflake_config.conf").create

    try {
      val properties = (new YamlPropertiesHelper).returnProperties("src/main/scala/io/phdata/snowpark/resources/application_properties.yaml")
      val schema = properties.getOrElse("Schema", "").asInstanceOf[String]
      val univariateMetricTests = generateUnivariateTests(properties.getOrElse("Univariate-Tests", "").asInstanceOf[String].split(",").toSeq)

      val univariateResults = runUnivariateTests(session, schema, univariateMetricTests)

      val resultSchema = StructType(
        StructField("metricRunID", StringType, nullable = true) ::
          StructField("metricName", StringType, nullable = true) ::
          StructField("jsonResults", StringType, nullable = true) ::
          Nil)

      val dfResults = session.createDataFrame(univariateResults, resultSchema)

      dfResults.write.mode(SaveMode.Overwrite).saveAsTable(properties.getOrElse("Metric-Table", "").asInstanceOf[String])

    }
    finally {
      session.close()
    }
  }

  /***
   * Will run all of the univariate tests against all of the columns in all of the tables on the schema.
   * @param session active session to snowflake.
   * @param schema Schema to get all tables from.
   * @param univariateMetricTests Tests to run against the columns
   * @return
   */
  def runUnivariateTests(session: Session, schema: String, univariateMetricTests: Seq[UnivariateMetric]): Seq[Row] = {
    val dfTables = session.table("INFORMATION_SCHEMA.TABLES")
      .filter(col("TABLE_SCHEMA") === schema && col("TABLE_TYPE") === "BASE TABLE")
      .select(col("TABLE_NAME")).limit(2)

    val tables = dfTables.collect().map(_.getString(0))

    tables.flatMap(table => {

      val columns = session.table("INFORMATION_SCHEMA.COLUMNS")
        .filter(col("TABLE_SCHEMA") === schema && col("TABLE_NAME") === table)
        .select(col("COLUMN_NAME")).collect().map(_.getString(0))

      val tableData = session.table(table)

      columns.flatMap(tableColumn => {
        univariateMetricTests.map(test => {
          val testResultRaw = test.runMetric("test", tableColumn, tableData)
          Row(testResultRaw.metricRunID, testResultRaw.metricName, testResultRaw.jsonResults)
        })
      })
    }).toSeq
  }

  /***
   * Will get the univariate metrics based on the string class passed in.
   * @param tests
   * @return
   */
  def generateUnivariateTests(tests: Seq[String]): Seq[UnivariateMetric] = {
    tests.toArray.map(x => {
      Class.forName("io.phdata.snowpark.metrics." + x).getDeclaredConstructor().newInstance().asInstanceOf[UnivariateMetric]
    }).toSeq
  }


}
