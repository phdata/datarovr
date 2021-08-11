package io.phdata.snowpark.metrics

import org.junit.Assert._
import org.junit.Test

class TestCorrelationMatrixMetric extends SessionConfigBase {

  @Test
  def testThreeColumnCorrelation(): Unit = {

    val df = getSession.createDataFrame(Seq((111, 111, 111), (2222, 2222, 2222), (1234, 5678, 9123))).toDF("first", "second", "third")

    val correlationMetric = new CorrelationMatrixMetric()
    val actual = correlationMetric.runMetric("test", df.schema.names.toList, df, "TestTable")

    val expected = MetricResult("test", "Correlation Matrix for - TestTable", "{\"FIRST::SECOND\":4.095028926522441e-01,\"FIRST::THIRD\":2.597438004514232e-01,\"SECOND::THIRD\":9.873619956567151e-01}")

    assertEquals(expected, actual)
  }

  @Test
  def testTwoColumnCorrelation(): Unit = {

    val df = getSession.createDataFrame(Seq((111, 111), (2222, 2222))).toDF("first", "second")

    val correlationMetric = new CorrelationMatrixMetric()
    val actual = correlationMetric.runMetric("test", df.schema.names.toList, df, "TestTable")

    val expected = MetricResult("test", "Correlation Matrix for - TestTable", "Empty Table/Not enough Number Columns")

    assertEquals(expected, actual)
  }

  @Test
  def testThreeColumnWithNullsCorrelation(): Unit = {

    val df = getSession.createDataFrame(Seq((111, 9876, 111), (2222, 123445, null.asInstanceOf[Int]))).toDF("first", "second", "third")

    val correlationMetric = new CorrelationMatrixMetric()
    val actual = correlationMetric.runMetric("test", df.schema.names.toList, df, "TestTable")

    val expected = MetricResult("test", "Correlation Matrix for - TestTable", "{\"FIRST::SECOND\":1.000000000000000e+00,\"FIRST::THIRD\":-1.000000000000000e+00,\"SECOND::THIRD\":-1.000000000000000e+00}")

    assertEquals(expected, actual)
  }

}

