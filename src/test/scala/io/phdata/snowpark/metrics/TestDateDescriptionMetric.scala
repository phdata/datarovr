package io.phdata.snowpark.metrics

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.junit.Assert._
import org.junit.Test


import com.snowflake.snowpark.Row
import com.snowflake.snowpark.types._

class TestDateDescriptionMetric extends SessionConfigBase {

  implicit val formats = DefaultFormats

  @Test
  def testMaxDate(): Unit = {
    val inputSchema = StructType(
      StructField("Number", IntegerType, nullable = true) ::
        StructField("Date", DateType, nullable = true) ::
        Nil)
    val df = getSession.createDataFrame(Seq(Row(5, "2021-03-22"), Row(10, "2021-03-03")), inputSchema)


    val dateTest = new DateDescriptionMetric()
    val actual = dateTest.runMetric("test", "Date", df)

    val expected = MetricResult("test", "Date Description Column - Date", "")
    val metric = "2021-03-22"

    val map = parse(actual.jsonResults).extract[Map[String, Any]]
    assertEquals(expected.metricRunID, actual.metricRunID)
    assertEquals(expected.metricName, actual.metricName)
    assertEquals(metric, map.getOrElse("DATE_MAX", 0))
  }

  @Test
  def testMinDate(): Unit = {
    val inputSchema = StructType(
      StructField("Number", IntegerType, nullable = true) ::
        StructField("Date", DateType, nullable = true) ::
        Nil)
    val df = getSession.createDataFrame(Seq(Row(5, "2021-03-22"), Row(10, "2021-03-03")), inputSchema)


    val dateTest = new DateDescriptionMetric()
    val actual = dateTest.runMetric("test", "Date", df)

    val expected = MetricResult("test", "Date Description Column - Date", "")
    val metric = "2021-03-03"

    val map = parse(actual.jsonResults).extract[Map[String, Any]]
    assertEquals(expected.metricRunID, actual.metricRunID)
    assertEquals(expected.metricName, actual.metricName)
    assertEquals(metric, map.getOrElse("DATE_MIN", 0))
  }

  @Test
  def testRangeDate(): Unit = {
    val inputSchema = StructType(
      StructField("Number", IntegerType, nullable = true) ::
        StructField("Date", DateType, nullable = true) ::
        Nil)
    val df = getSession.createDataFrame(Seq(Row(5, "2021-03-22"), Row(10, "2021-03-03")), inputSchema)


    val dateTest = new DateDescriptionMetric()
    val actual = dateTest.runMetric("test", "Date", df)

    val expected = MetricResult("test", "Date Description Column - Date", "")
    val metric = BigInt(19)

    val map = parse(actual.jsonResults).extract[Map[String, Any]]
    assertEquals(expected.metricRunID, actual.metricRunID)
    assertEquals(expected.metricName, actual.metricName)
    assertEquals(metric, map.getOrElse("DATE_RANGE", 0))
  }
}

