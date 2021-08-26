package io.phdata.snowpark.metrics

import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.types.StructField
import io.phdata.snowpark.helpers.TableName

trait MetricObject {
  /**
   * Builds a metric instance from a table name and dataframe (representing a table)
   * @param tn TableName of the table to profile
   * @param df DataFrame reference to the table to profile
   * @return Metric for the table
   */
  def apply(tn: TableName, df: DataFrame): Option[Metric]

  /**
   * Tests if the passed column is a numeric type
   * @param column column to test
   * @return
   */
  def isNumeric(column: StructField): Boolean = {
    val numberTypes = Seq("integer", "long", "float", "double", "decimal")
    numberTypes.map(t => {
      column.dataType.typeName.toLowerCase.startsWith(t)
    }).reduce(_ || _)
  }

  /**
   * Tests if the passed column is a date or timstamp type
   * @param column column to test
   * @return
   */
  def isDate(column: StructField): Boolean = {
    val dateTypes = Seq("date", "datetime", "timestamp")
    dateTypes.contains(column.dataType.typeName.toLowerCase)
  }

}
