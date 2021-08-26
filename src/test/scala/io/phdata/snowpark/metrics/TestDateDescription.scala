package io.phdata.snowpark.metrics

import com.snowflake.snowpark.Row
import org.junit.Assert._
import org.junit.Test
import io.phdata.snowpark.helpers.TableName

import java.sql.Timestamp

class TestDateDescription extends SessionConfigBase {

  @Test
  def testDates(): Unit = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

    val df = getSession.createDataFrame(Seq[(Int, Timestamp)](
      (5, new Timestamp(format.parse("2021-03-22").getTime)),
      (10, new Timestamp(format.parse("2021-03-03").getTime))
    )).toDF("number", "date")

    val metric = DateDescription(TableName("one", "two", "three"), df)
    metric match {
      case Some(result) =>
        val actual = result.values
          .drop("timestamp")
          .collect()
          .toSeq

        val expected = Seq(
          Row("one", "two", "three", "DATE", new Timestamp(format.parse("2021-03-22").getTime),
            new Timestamp(format.parse("2021-03-03").getTime), 19,
          ),
        )

        assertEquals(expected, actual)
      case None => fail("metric did not return dataframe")
    }
  }

  @Test
  def testNoDateColumns(): Unit = {
    val df = getSession.createDataFrame(Seq((5, "foo"), (10, "bar"))).toDF("number", "character")

    val metric = DateDescription(TableName("one", "two", "three"), df)

    assertTrue(metric.isEmpty)
  }

  @Test
  def testEmptyDF(): Unit = {
    val df = getSession.createDataFrame(Seq.empty[(Int, String)]).toDF("number", "character")

    val metric = DateDescription(TableName("one", "two", "three"), df)

    assertTrue(metric.isEmpty)
  }
}