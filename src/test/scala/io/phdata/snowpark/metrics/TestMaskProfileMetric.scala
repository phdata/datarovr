package io.phdata.snowpark.metrics

import io.phdata.snowpark.metrics.MetricResult
import org.junit.Assert._
import org.junit.Test

class TestMaskProfileMetric extends TestingSessionConfigBase {

  @Test
  def testCharacterReplacement(): Unit = {

    val df = getSession.createDataFrame(Seq((111, "one"), (2222, "two"))).toDF("Number", "character")

    val maskProfileMetric = new MaskProfileMetric()
    val actual = maskProfileMetric.runMetric("test", "character", df)

    val expected = MetricResult("test", "Mask Profile Column - character", "{\"CHARACTER\":\"XXX\",\"COUNT\":2}")

    assertEquals(actual, expected)
  }

  @Test
  def testNumberReplacement(): Unit = {

    val df = getSession.createDataFrame(Seq((111, "one"), (222, "two"))).toDF("Number", "character")

    val maskProfileMetric = new MaskProfileMetric()
    val actual = maskProfileMetric.runMetric("test", "Number", df)

    val expected = MetricResult("test", "Mask Profile Column - Number", "{\"COUNT\":2,\"NUMBER\":\"NNN\"}")

    assertEquals(actual, expected)
  }

  @Test
  def testComplexReplacement(): Unit = {

    val df = getSession.createDataFrame(Seq("125-266-1234", "215-266-9876", "five-five-five")).toDF("phone")

    val maskProfileMetric = new MaskProfileMetric()
    val actual = maskProfileMetric.runMetric("test", "phone", df)

    val expected = MetricResult("test", "Mask Profile Column - phone", "{\"COUNT\":2,\"PHONE\":\"NNN-NNN-NNNN\"} {\"COUNT\":1,\"PHONE\":\"XXXX-XXXX-XXXX\"}")

    assertEquals(actual, expected)
  }

  @Test
  def testEmptyDF(): Unit = {

    val df = getSession.createDataFrame(Seq.empty[(String, String)]).toDF("Number", "character")

    val maskProfileMetric = new MaskProfileMetric()
    val actual = maskProfileMetric.runMetric("test", "Number", df)

    val expected = MetricResult("test", "Mask Profile Column - Number", "Empty Table Or Null Column")

    assertEquals(actual, expected)
  }

  @Test
  def testEmptyColumnInDF(): Unit = {

    val df = getSession.createDataFrame(Seq((111, null.asInstanceOf[String]), (222, null.asInstanceOf[String]))).toDF("Number", "character")

    val maskProfileMetric = new MaskProfileMetric()
    val actual = maskProfileMetric.runMetric("test", "character", df)

    val expected = MetricResult("test", "Mask Profile Column - character", "Empty Table Or Null Column")

    assertEquals(actual, expected)
  }


  @Test
  def testEmptyRecordInDF(): Unit = {

    val df = getSession.createDataFrame(Seq((111, "one"), (222, null.asInstanceOf[String]))).toDF("Number", "character")

    val maskProfileMetric = new MaskProfileMetric()
    val actual = maskProfileMetric.runMetric("test", "character", df)

    val expected = MetricResult("test", "Mask Profile Column - character", "{\"CHARACTER\":\"XXX\",\"COUNT\":1}")

    assertEquals(actual, expected)
  }
}

