package io.phdata.snowpark.metrics

import com.snowflake.snowpark.{DataFrame, Window}
import com.snowflake.snowpark.functions.{builtin, col, corr, current_timestamp, lit}
import com.snowflake.snowpark.types.DataTypes
import io.phdata.snowpark.algorithms.{Accumulator, NonRepeatingCombPull, PermComb}
import io.phdata.snowpark.helpers.TableName
import plotly.layout.{Axis, Layout}
import plotly.{Config, Heatmap, Plotly, Range}

import java.sql.Timestamp

class CorrelationMatrix(df: DataFrame) extends Metric {
  //TODO: validate columns of dataframe
  override val values: DataFrame = df
  override val tableSuffix: String = "correlation_matrix"

  override def latestValues: DataFrame = {
    val first = builtin("first_value")

    val ws = Window.partitionBy(
      col("database"),
      col("schema"),
      col("table"),
      col("column"),
    ).orderBy(col("timestamp").desc)

    values
      .select(
        first(col("database")).over(ws).as("database"),
        first(col("schema")).over(ws).as("schema"),
        first(col("table")).over(ws).as("table"),
        first(col("column")).over(ws).as("column"),
        first(col("timestamp")).over(ws).as("timestamp"),
        first(col("correlation")).over(ws).as("correlation"),
      )
  }

  private def HTMLTemplate(id: String, js: String): String = {
    s"""<div class="correlation_matrix" id="$id">
       |</div>
       |<script>
       |$js
       |</script>
       |""".stripMargin
  }

  override def getHTMLSnippet: String = {
    val nl = System.lineSeparator()

    val groups = latestValues
      .collect()
      .toSeq
      .map(r => {
        val database = r.getString(0)
        val schema = r.getString(1)
        val table = r.getString(2)
        val columns = r.getString(3)
        val timestamp = r.getTimestamp(4)
        val correlation = r.getDouble(5)

        ((database, schema, table), (columns, timestamp, correlation))
      })
      .groupBy(_._1)

    val tableOptions = groups
      .keySet
      .map(t => {
        val table = t.productIterator.mkString(".")
        s"""<option value="${table.replace(".", "-")}">$table</option>$nl"""
      })

    val divs = groups.map(g => {
      val table = Seq(g._1._1, g._1._2, g._1._3).mkString(".")
      var timestamp = new Timestamp(0)

      val groups = g._2.flatMap(cors => {
        timestamp = cors._2._2
        val cols = cors._2._1.split("::")

        Seq(
          (cols(0), (cols(1), cors._2._3)),
          (cols(1), (cols(0), cors._2._3)),
          (cols(0), (cols(0), 1.0)),
          (cols(1), (cols(1), 1.0)),
        )
      })
        .distinct
        .groupBy(_._1)

      val matrix = groups
        .map(g => {
          val clist = g._2.map(_._2)
            .sortBy(_._1)
            .map(_._2)
          (g._1, clist)
        })
        .toSeq
        .sortBy(_._1)
        .map(_._2)

      val cols = groups
        .keySet
        .toSeq
        .sortBy(identity)

      val hm = Heatmap(z=matrix.reverse, x=cols, y=cols.reverse)

      val id = table.replace(".", "-")

      val xaxis = Axis().withAutomargin(true)
      val yaxis = Axis().withAutomargin(true)

      val lo = Layout()
        .withTitle(s"Correlation Matrix as of $timestamp")
        .withAutosize(true)
        .withYaxis(yaxis)
        .withXaxis(xaxis)

      HTMLTemplate(id, Plotly.jsSnippet(id, Seq(hm), lo, Config()))
    }).mkString

    s"""
       |<style>
       |  .correlation_matrix {
       |    display: none;
       |  }
       |</style>
       |<script>
       |  $$(function() {
       |    $$("#current_correlation_matrix_table").change(function() {
       |      const plotDiv = "#"+$$(this).val();
       |      $$(".correlation_matrix").hide();
       |      $$(plotDiv).show();
       |    });
       |  });
       |</script>
       |<select id="current_correlation_matrix_table">
       |  <option disabled selected value>-- select table --</option>
       |  $tableOptions
       |</select>
       |$divs
       |""".stripMargin
  }
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