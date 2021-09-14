package io.phdata.snowpark.metrics

import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions.{col, corr, current_timestamp, lit}
import com.snowflake.snowpark.types.DataTypes
import io.phdata.snowpark.algorithms.{Accumulator, NonRepeatingCombPull, PermComb}
import io.phdata.snowpark.helpers.TableName

class CorrelationMatrix(df: DataFrame) extends Metric {
  //TODO: validate columns of dataframe
  override val values: DataFrame = df
  override val tableSuffix: String = "correlation_matrix"
}

object CorrelationMatrix extends MetricObject {
  override def apply(tn: TableName, table: DataFrame): Option[CorrelationMatrix] = {
    val numeric_cols = table.schema.fields.filter(isNumeric).map(_.name)

    val accumulator = new Accumulator[String]
    val nonRepeatingCombPull = new NonRepeatingCombPull[String](numeric_cols.toList, 2)
    PermComb.constructPushGenerator(accumulator.push, nonRepeatingCombPull.iterator)

    val col_combinations = accumulator.listAccu.toArray

    val correlations = table.na.drop(0, numeric_cols)
      .agg(
        col_combinations.map(x => {
          corr(col(x.head).cast(DataTypes.DoubleType), col(x.last).cast(DataTypes.DoubleType)).as(x.mkString("::"))
        })
      )

    val values = col_combinations.map(x => {
      correlations.select(
        lit(tn.database).as("database"),
        lit(tn.schema).as("schema"),
        lit(tn.table).as("table"),
        lit(x.mkString("::")).as("column"),
        current_timestamp().as("timestamp"),
        col(x.mkString("::")).as("correlation"),
      )
    }).reduceOption(_ union _)

    values match {
      case Some(df) => Some(new CorrelationMatrix(df))
      case None => None
    }
  }
}