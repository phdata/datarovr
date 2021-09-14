package io.phdata.snowpark.helpers

import com.snowflake.snowpark.{DataFrame, SaveMode, Session}
import com.snowflake.snowpark.functions._
import io.phdata.snowpark.metrics.{Metric, MetricObject}

import java.io.{BufferedWriter, File, FileWriter}
import scala.reflect.runtime.{universe => ru}

object functions {
  /**
   * Gets a Seq of TableNames that match the --tables glob
   * @param conf implicit configuration
   * @param session implicit snowflake session
   * @return
   */
  def getMatchingTables()(implicit conf: Config, session: Session): Seq[TableName] = {
    val tn = TableName(conf.tables.get)

    val matches = udf((data: String, pattern: String) => {
      Glob(pattern).r.findAllMatchIn(data).nonEmpty
    })

    val dbs = session.sql("show databases")
      .select(col("\"name\""))
      .filter(matches(col("\"name\""), lit(tn.database)))
      .collect()
      .map(_.getString(0))

    dbs.flatMap(d => {
      session
        .sql(s"select table_schema, table_name from $d.information_schema.tables")
        .filter(
          matches(col("TABLE_SCHEMA"), lit(tn.schema)) &&
          matches(col("TABLE_NAME"), lit(tn.table))
        )
        .collect()
        .map(r => (r.getString(0), r.getString(1)))
        .map(st => TableName(d, st._1, st._2))
    })
  }

  /***
   * Saves the provided dataframe to the provided table name in Snowflake
   * @param tableName name of the table to save the metrics in
   * @param df dataframe of generated metrics
   */
  def appendToSnowflakeTable(tableName: TableName, df: DataFrame): Unit = {
    df.write.mode(SaveMode.Append).saveAsTable(tableName.asMultipartName)
  }

  /**
   * Takes a class name as a string and returns that classes companion object
   * @param s
   * @return
   */
  def companionFromString(s: String): MetricObject = {
    val cls = Class.forName("io.phdata.snowpark.metrics."+s)
    val rm = ru.runtimeMirror(cls.getClassLoader)
    rm.reflectModule(rm.classSymbol(cls).companion.asModule).instance.asInstanceOf[MetricObject]
  }

  def writeReport(file: File, metrics: Seq[Metric]): Unit = {
    val nl = System.lineSeparator()
    val b = new BufferedWriter(new FileWriter(file))

    b.write(scala.io.Source.fromResource("report_start.html").getLines().mkString(nl))
    b.write(s"""<label for="current_metric">Metric:</label>$nl""")
    b.write(s"""<select id="current_metric">$nl""")
    b.write(s"<option disabled selected value> -- choose a metric --</option>$nl")
    b.write(s"</select>$nl")
    b.write(s"<hr>$nl")

    metrics.foreach(m => {
      b.write(s"""<script>$$("#current_metric").append("<option value=\\"${m.name}\\">${m.name}</option>")</script>$nl""")
      b.write(s"""<div class="metric" id="${m.name}">$nl""")
      b.write(m.getHTMLSnippet)
      b.write(s"</div>$nl")
    })

    b.write(scala.io.Source.fromResource("report_end.html").getLines().mkString(nl))
    b.close()
  }
}
