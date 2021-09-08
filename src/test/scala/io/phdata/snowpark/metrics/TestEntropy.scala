package io.phdata.snowpark.metrics

import com.snowflake.snowpark.Row
import com.snowflake.snowpark.functions.col
import io.phdata.snowpark.helpers.TableName
import org.junit.Assert.{assertEquals, fail}
import org.junit.Test

class TestEntropy extends SessionConfigBase {

  @Test
  def TestEntropyOne(): Unit = {

    val df = getSession.createDataFrame(Seq((5, "one"), (10, "two"))).toDF("Number", "character")

    val metric = Entropy(TableName("one", "two", "three"), df)
    metric match {
      case Some(result) =>
        val actual = result.values
          .drop(col("timestamp"))
          .sort(col("column"))
          .collect()
          .toSeq

        val expected = Seq(
          Row("one", "two", "three", "NUMBER", 1.0),
        )

        assertEquals(expected, actual)
      case None => fail("metric did not return a dataframe")
    }
  }

  @Test
  def TestEntropyTwo(): Unit = {

    val df = getSession.createDataFrame(Seq(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)).toDF("Number")

    val metric = Entropy(TableName("one", "two", "three"), df)
    metric match {
      case Some(result) =>
        val actual = result.values
          .drop(col("timestamp"))
          .sort(col("column"))
          .collect()
          .toSeq

        val expected = Seq(
          Row("one", "two", "three", "NUMBER", 2.321928094887362),
        )

        assertEquals(expected, actual)
      case None => fail("metric did not return a dataframe")
    }
  }
}
