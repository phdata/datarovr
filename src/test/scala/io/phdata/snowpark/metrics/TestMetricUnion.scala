package io.phdata.snowpark.metrics

import com.snowflake.snowpark.Row
import com.snowflake.snowpark.functions.col
import io.phdata.snowpark.helpers.TableName
import org.junit.Assert.{assertEquals, fail}
import org.junit.Test

class TestMetricUnion extends SessionConfigBase {

  @Test
  def testCorrelationMatrixMetricUnion(): Unit = {
    val df1 = getSession.createDataFrame(Seq((111, 111, 111), (222, 222, 222))).toDF("first", "second", "third")
    val df2 = getSession.createDataFrame(Seq((333, 333, 333), (444, 444, 444))).toDF("first", "second", "third")

    val metric1 = CorrelationMatrix(TableName("one", "two", "three"), df1)
    val metric2 = CorrelationMatrix(TableName("four", "five", "six"), df2)

    metric1 match {
      case Some(m1) =>
        metric2 match {
          case Some(m2) =>
            val actual = m1.union(m2).values
                           .drop("timestamp")
                           .sort(col("column"), col("database"))
                           .collect()
                           .toSeq

            val expected = Seq(
              Row("four", "five", "six", "FIRST::SECOND", 1.0),
              Row("one", "two", "three", "FIRST::SECOND", 1.0),
              Row("four", "five", "six", "FIRST::THIRD", 1.0),
              Row("one", "two", "three", "FIRST::THIRD", 1.0),
              Row("four", "five", "six", "SECOND::THIRD", 1.0),
              Row("one", "two", "three", "SECOND::THIRD", 1.0),
            )

           assertEquals(expected, actual)
          case None => fail("metric2 was not a metric")
        }
      case None => fail("metric1 was not a metric")
    }
  }

}
