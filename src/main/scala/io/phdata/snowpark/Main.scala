package io.phdata.snowpark

import com.snowflake.snowpark.functions.col
import com.snowflake.snowpark.types.{StringType, StructField, StructType}
import com.snowflake.snowpark.{Row, SaveMode, Session}
import io.phdata.snowpark.helpers.EnvPropertyHelper
import io.phdata.snowpark.metrics._

object Main {

  def main(args: Array[String]) {

    val session = Session.builder.configs((new EnvPropertyHelper).getSnowflakeConnectionProperties()).create

    try {
      val properties = (new EnvPropertyHelper).getApplicationProperties
      val schema = properties.getOrElse("SCHEMA", "")
      val univariateMetricTests = generateUnivariateTest(properties.getOrElse("UNIVARIATE_TESTS", "").split(",").toSeq)

      val univariateResults = runUnivariateTests(session, schema, univariateMetricTests)

      val multivariateMetricTests = generateMultivariateTest(properties.getOrElse("MULTIVARIATE_TESTS", "").asInstanceOf[String].split(",").toSeq)

      val multivariateResults = runMultivariateTests(session, schema, multivariateMetricTests)

      val resultSchema = StructType(
        StructField("metricRunID", StringType, nullable = true) ::
          StructField("metricName", StringType, nullable = true) ::
          StructField("jsonResults", StringType, nullable = true) ::
          Nil)

      val dfResultsUnivariate = session.createDataFrame(univariateResults, resultSchema)
      val dfResultsMultivariate = session.createDataFrame(multivariateResults, resultSchema)

      val dfResults = dfResultsUnivariate.unionAll(dfResultsMultivariate)

      dfResults.write.mode(SaveMode.Overwrite).saveAsTable(properties.getOrElse("METRIC_TABLE", "").asInstanceOf[String])

    }
    finally {
      session.close()
    }
  }

  /***
   * Will run all of the multiivariate tests against all of the columns in all of the tables on the schema.
   * @param session active session to snowflake.
   * @param schema Schema to get all tables from.
   * @param multivariateMetricTests Tests to run against the columns
   * @return
   */
  def runMultivariateTests(session: Session, schema: String, multivariateMetricTests: Seq[MultivariateMetric]): Seq[Row] = {
    val dfTables = session.table("INFORMATION_SCHEMA.TABLES")
      .filter(col("TABLE_SCHEMA") === schema && col("TABLE_TYPE") === "BASE TABLE")
      .select(col("TABLE_NAME")).limit(2)

    val tables = dfTables.collect().map(_.getString(0))

    tables.flatMap(table => {

      val columns = session.table("INFORMATION_SCHEMA.COLUMNS")
        .filter(col("TABLE_SCHEMA") === schema && col("TABLE_NAME") === table)
        .select(col("COLUMN_NAME")).collect().map(_.getString(0))

      val tableData = session.table(table)

        multivariateMetricTests.map(test => {
          val testResultRaw = test.runMetric("test", columns.toList, tableData, table)
          Row(testResultRaw.metricRunID, testResultRaw.metricName, testResultRaw.jsonResults)
        })
    }).toSeq
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
      .select(col("TABLE_NAME")).limit(1)

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
   * Will get the metrics tests based on the string class passed in.
   * @param tests
   * @return
   */
  def generateUnivariateTest(tests: Seq[String]): Seq[UnivariateMetric] = {
    tests.toArray.map(x => {
      Class.forName("io.phdata.snowpark.metrics." + x).getDeclaredConstructor().newInstance().asInstanceOf[UnivariateMetric]
    }).toSeq
  }

  /***
   * Will get the metrics tests based on the string class passed in.
   * @param tests
   * @return
   */
  def generateMultivariateTest(tests: Seq[String]): Seq[MultivariateMetric] = {
    tests.toArray.map(x => {
      Class.forName("io.phdata.snowpark.metrics." + x).getDeclaredConstructor().newInstance().asInstanceOf[MultivariateMetric]
    }).toSeq
  }


}
