package io.phdata.snowpark.metrics

import com.snowflake.snowpark.Row
import com.snowflake.snowpark.functions.col
import io.phdata.snowpark.helpers.TableName
import org.junit.Assert._
import org.junit.Test

class TestNumberDescription extends SessionConfigBase {

  @Test
  def TestNumberDescription(): Unit = {
    val df = getSession.createDataFrame(Seq((5, "one"), (10, "two"))).toDF("Number", "character")

    val metric = NumberDescription(TableName("one", "two", "three"), df)
    metric match {
      case Some(result) =>
        val actual = result.values
          .drop(col("timestamp"))
          .sort(col("column"))
          .collect()
          .toSeq

        val expected = Seq(
          Row("one", "two", "three", "NUMBER", 3.5355339059327378, 7.5, 10.0, 5.0, 12.5,
            null, null, 7.5, 7.5, 12.5, 5.0, 0.4714045207910317, 15.0, 2
          ),
        )

        assertEquals(expected, actual)
      case None => fail("metric did not return a dataframe")
    }
  }
}

