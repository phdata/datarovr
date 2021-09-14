package io.phdata.snowpark.metrics

import com.snowflake.snowpark.Row
import com.snowflake.snowpark.functions.col
import io.phdata.snowpark.helpers.TableName
import org.junit.Assert._
import org.junit.Test

class TestMaskProfile extends SessionConfigBase {

  @Test
  def testCharacterReplacement(): Unit = {

    val df = getSession.createDataFrame(Seq((111, "one"), (2222, "two"))).toDF("Number", "character")

    val metric = MaskProfile(TableName("one", "two", "three"), df)
    metric match {
      case Some(results) =>
        val actual = results.values
          .drop("timestamp")
          .sort(col("column"), col("mask"))
          .collect()
          .toSeq

        val expected = Seq(
          Row("one", "two", "three", "CHARACTER", "XXX", 1.0),
          Row("one", "two", "three", "NUMBER", "NNN", 0.5),
          Row("one", "two", "three", "NUMBER", "NNNN", 0.5),
        )

        assertEquals(expected, actual)
      case None => fail("metric did not return a dataframe")
    }

  }

  @Test
  def testComplexReplacement(): Unit = {

    val df = getSession.createDataFrame(Seq("125-266-1234", "215-266-9876", "five-five-five", "four-four-four")).toDF("phone")

    val metric = MaskProfile(TableName("one", "two", "three"), df)
    metric match {
      case Some(results) =>
        val actual = results.values
          .drop("timestamp")
          .sort(col("mask"))
          .collect()
          .toSeq

        val expected = Seq(
          Row("one", "two", "three", "PHONE", "NNN-NNN-NNNN", 0.5),
          Row("one", "two", "three", "PHONE", "XXXX-XXXX-XXXX", 0.5),
        )

        assertEquals(expected, actual)
      case None => fail("metric did not return a dataframe")
    }

  }

  @Test
  def testEmptyDF(): Unit = {

    val df = getSession.createDataFrame(Seq.empty[(Int, String)]).toDF("Number", "character")

    val metric = MaskProfile(TableName("one", "two", "three"), df)

    assertTrue(metric.isEmpty)
  }

  @Test
  def testEmptyColumnInDF(): Unit = {

    val df = getSession.createDataFrame(Seq((111, null.asInstanceOf[String]), (222, null.asInstanceOf[String]))).toDF("Number", "character")

    val metric = MaskProfile(TableName("one", "two", "three"), df)
    metric match {
      case Some(results) =>
        val actual = results.values
          .drop("timestamp")
          .collect()
          .toSeq

        val expected = Seq(
          Row("one", "two", "three", "NUMBER", "NNN", 1.0),
        )

        assertEquals(expected, actual)
      case None => fail("metric did not return a dataframe")
    }

  }

  @Test
  def testEmptyRecordInDF(): Unit = {

    val df = getSession.createDataFrame(Seq((111, "one"), (222, null.asInstanceOf[String]))).toDF("Number", "character")

    val metric = MaskProfile(TableName("one", "two", "three"), df)
    metric match {
      case Some(results) =>
        val actual = results.values
          .drop("timestamp")
          .sort(col("mask"))
          .collect()
          .toSeq

        val expected = Seq(
          Row("one", "two", "three", "NUMBER", "NNN", 1.0),
          Row("one", "two", "three", "CHARACTER", "XXX", 1.0),
        )

        assertEquals(expected, actual)
      case None => fail("metric did not return a dataframe")
    }
  }
}

