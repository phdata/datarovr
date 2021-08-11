package io.phdata.snowpark.metrics

import org.junit.Assert._
import org.junit.Test
import org.json4s._
import org.json4s.jackson.JsonMethods._

class TestNumberDescriptionProfileMetric extends SessionConfigBase {

  implicit val formats = DefaultFormats

  @Test
  def testStandardDeviation(): Unit = {
    val df = getSession.createDataFrame(Seq((5, "one"), (10, "two"))).toDF("Number", "character")

    val integerTest = new NumberDescriptionMetric()
    val actual = integerTest.runMetric("test", "Number", df)

    val expected = MetricResult("test", "Number Description Column - Number", "")
    val metric = 3.535533905932738e+00

    val map = parse(actual.jsonResults).extract[Map[String, Any]]
    assertEquals(expected.metricRunID, actual.metricRunID)
    assertEquals(expected.metricName, actual.metricName)
    assertEquals(metric, map.getOrElse("STDDEV", 0))
  }

  @Test
  def testMean(): Unit = {
    val df = getSession.createDataFrame(Seq((5, "one"), (10, "two"))).toDF("Number", "character")

    val integerTest = new NumberDescriptionMetric()
    val actual = integerTest.runMetric("test", "Number", df)

    val expected = MetricResult("test", "Number Description Column - Number", "")
    val metric = 7.5

    val map = parse(actual.jsonResults).extract[Map[String, Any]]
    assertEquals(expected.metricRunID, actual.metricRunID)
    assertEquals(expected.metricName, actual.metricName)
    assertEquals(metric, map.getOrElse("MEAN", 0))
  }

  @Test
  def testMax(): Unit = {
    val df = getSession.createDataFrame(Seq((5, "one"), (10, "two"))).toDF("Number", "character")

    val integerTest = new NumberDescriptionMetric()
    val actual = integerTest.runMetric("test", "Number", df)

    val expected = MetricResult("test", "Number Description Column - Number", "")
    val metric = BigInt(10)

    val map = parse(actual.jsonResults).extract[Map[String, Any]]
    assertEquals(expected.metricRunID, actual.metricRunID)
    assertEquals(expected.metricName, actual.metricName)
    assertEquals(metric, map.getOrElse("MAX", 0))
  }

  @Test
  def testMin(): Unit = {
    val df = getSession.createDataFrame(Seq(5, 10, 15, 20, 25)).toDF("Number")

    val integerTest = new NumberDescriptionMetric()
    val actual = integerTest.runMetric("test", "Number", df)

    val expected = MetricResult("test", "Number Description Column - Number", "")
    val metric = BigInt(5)

    val map = parse(actual.jsonResults).extract[Map[String, Any]]
    assertEquals(expected.metricRunID, actual.metricRunID)
    assertEquals(expected.metricName, actual.metricName)
    assertEquals(metric, map.getOrElse("MIN", 0))
  }

  @Test
  def testKurtosis(): Unit = {
    val df = getSession.createDataFrame(Seq(5, 10, 15, 20, 25)).toDF("Number")

    val integerTest = new NumberDescriptionMetric()
    val actual = integerTest.runMetric("test", "Number", df)

    val expected = MetricResult("test", "Number Description Column - Number", "")
    val metric = -1.2

    val map = parse(actual.jsonResults).extract[Map[String, Any]]
    assertEquals(expected.metricRunID, actual.metricRunID)
    assertEquals(expected.metricName, actual.metricName)
    assertEquals(metric, map.getOrElse("KURTOSIS", 0))
  }

  @Test
  def testSkew(): Unit = {
    val df = getSession.createDataFrame(Seq(5, 6, 15, 20, 25)).toDF("Number")

    val integerTest = new NumberDescriptionMetric()
    val actual = integerTest.runMetric("test", "Number", df)

    val expected = MetricResult("test", "Number Description Column - Number", "")
    val metric = 0.07925506585740152

    val map = parse(actual.jsonResults).extract[Map[String, Any]]
    assertEquals(expected.metricRunID, actual.metricRunID)
    assertEquals(expected.metricName, actual.metricName)
    assertEquals(metric, map.getOrElse("SKEWNESS", 0))
  }

  @Test
  def testPercentiles(): Unit = {
    val df = getSession.createDataFrame(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).toDF("Number")

    val integerTest = new NumberDescriptionMetric()
    val actual = integerTest.runMetric("test", "Number", df)

    val expected = MetricResult("test", "Number Description Column - Number", "")

    val map = parse(actual.jsonResults).extract[Map[String, Any]]
    assertEquals(expected.metricRunID, actual.metricRunID)
    assertEquals(expected.metricName, actual.metricName)
    assertEquals(3.5, map.getOrElse("PERCENTILE25", 0))
    assertEquals(5.5, map.getOrElse("PERCENTILE50", 0))
    assertEquals(8.5, map.getOrElse("PERCENTILE75", 0))
  }

  @Test
  def testIQR(): Unit = {
    val df = getSession.createDataFrame(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).toDF("Number")

    val integerTest = new NumberDescriptionMetric()
    val actual = integerTest.runMetric("test", "Number", df)

    val expected = MetricResult("test", "Number Description Column - Number", "")

    val map = parse(actual.jsonResults).extract[Map[String, Any]]
    assertEquals(expected.metricRunID, actual.metricRunID)
    assertEquals(expected.metricName, actual.metricName)
    assertEquals(BigInt(5), map.getOrElse("IQR", 0))
  }

  @Test
  def testCV(): Unit = {
    val df = getSession.createDataFrame(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).toDF("Number")

    val integerTest = new NumberDescriptionMetric()
    val actual = integerTest.runMetric("test", "Number", df)

    val expected = MetricResult("test", "Number Description Column - Number", "")

    val map = parse(actual.jsonResults).extract[Map[String, Any]]
    assertEquals(expected.metricRunID, actual.metricRunID)
    assertEquals(expected.metricName, actual.metricName)
    assertEquals(0.5504818925719417, map.getOrElse("CV", 0))
  }

  @Test
  def testSumCount(): Unit = {
    val df = getSession.createDataFrame(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).toDF("Number")

    val integerTest = new NumberDescriptionMetric()
    val actual = integerTest.runMetric("test", "Number", df)

    val expected = MetricResult("test", "Number Description Column - Number", "")

    val map = parse(actual.jsonResults).extract[Map[String, Any]]
    assertEquals(expected.metricRunID, actual.metricRunID)
    assertEquals(expected.metricName, actual.metricName)
    assertEquals(BigInt(55), map.getOrElse("SUM", 0))
    assertEquals(BigInt(10), map.getOrElse("COUNT", 0))
  }

  @Test
  def testEntropy(): Unit = {
    val df = getSession.createDataFrame(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).toDF("Number")

    val integerTest = new NumberDescriptionMetric()
    val actual = integerTest.runMetric("test", "Number", df)

    val expected = MetricResult("test", "Number Description Column - Number", "")

    val map = parse(actual.jsonResults).extract[Map[String, Any]]
    assertEquals(expected.metricRunID, actual.metricRunID)
    assertEquals(expected.metricName, actual.metricName)
    assertEquals("3.321928094887362", map.getOrElse("ENTROPY", 0))
  }

}

