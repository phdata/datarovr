package io.phdata.snowpark.metrics

import com.snowflake.snowpark.Row
import com.snowflake.snowpark.functions.col
import io.phdata.snowpark.helpers.TableName
import org.junit.Assert._
import org.junit.Test

class TestCorrelationMatrix extends SessionConfigBase {

  @Test
  def testThreeColumnCorrelation(): Unit = {

    val df = getSession.createDataFrame(Seq((111, 111, 111), (2222, 2222, 2222), (1234, 5678, 9123))).toDF("first", "second", "third")

    val metric = CorrelationMatrix(TableName("one", "two", "three"), df)
    metric match {
      case Some(result) =>

        val actual = result.values
          .drop("timestamp")
          .sort(col("column"))
          .collect()
          .toSeq

        val expected = Seq(
          Row("one", "two", "three", "FIRST::SECOND", 4.095028926522441e-01),
          Row("one", "two", "three", "FIRST::THIRD", 2.597438004514232e-01),
          Row("one", "two", "three", "SECOND::THIRD", 9.873619956567151e-01),
        )

        assertEquals(expected, actual)

      case None => fail("metric did not return a dataframe")
    }
  }

  @Test
  def testTwoColumnCorrelation(): Unit = {

    val df = getSession.createDataFrame(Seq((111, 111), (2222, 2222))).toDF("first", "second")

    val metric = CorrelationMatrix(TableName("one", "two", "three"), df)

    assertTrue(metric.isEmpty)
  }

  @Test
  def testThreeColumnWithNullsCorrelation(): Unit = {

    val df = getSession.createDataFrame(Seq((111, 9876, 111), (2222, 123445, null.asInstanceOf[Int]))).toDF("first", "second", "third")

    val metric = CorrelationMatrix(TableName("one", "two", "three"), df)
    metric match {
      case Some(result) =>
        val actual = result.values
          .drop("timestamp")
          .sort(col("column"))
          .collect()
          .toSeq

        val expected = Seq(
          Row("one", "two", "three", "FIRST::SECOND", 1.000000000000000e+00),
          Row("one", "two", "three", "FIRST::THIRD", -1.000000000000000e+00),
          Row("one", "two", "three", "SECOND::THIRD", -1.000000000000000e+00),
        )

        assertEquals(expected, actual)
      case None => fail("metric did not return a dataframe")
    }
  }
}