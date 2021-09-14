package io.phdata.snowpark.metrics

import com.snowflake.snowpark.{DataFrame, Window}
import com.snowflake.snowpark.functions._
import io.phdata.snowpark.helpers.TableName
import plotly.Bar
import plotly.Plotly
import plotly.layout.Layout
import plotly.Config
import plotly.element.Orientation
import plotly.layout.Axis

class MaskProfile(df: DataFrame) extends Metric {
  val nl = System.lineSeparator()
  //TODO: validate columns of dataframe
  override val values: DataFrame = df
  override val tableSuffix: String = "mask_profile"

  def latestValues: DataFrame = {
    val first = builtin("first_value")

    val ws = Window.partitionBy(
      col("database"),
      col("schema"),
      col("table"),
      col("column"),
      col("mask"),
    ).orderBy(col("timestamp").desc)

    values
      .select(
        first(col("database")).over(ws).as("database"),
        first(col("schema")).over(ws).as("schema"),
        first(col("table")).over(ws).as("table"),
        first(col("column")).over(ws).as("column"),
        first(col("timestamp")).over(ws).as("timestamp"),
        first(col("mask")).over(ws).as("mask"),
        first(col("percentage")).over(ws).as("percentage"),
      )
  }

  private def HTMLTemplate(id: String, js: String): String = {
    s"""<div class="mask_profile" id="$id">
       |</div>
       |<script>
       |$js
       |</script>
       |""".stripMargin
  }

  override def getHTMLSnippet: String = {
    val groups = latestValues
      .collect()
      .toSeq
      .map(r => {
        val database = r.getString(0)
        val schema = r.getString(1)
        val table = r.getString(2)
        val column = r.getString(3)
        val timestamp = r.getTimestamp(4)
        val mask = r.getString(5)
        val percentage = r.getDouble(6)

        ((database, schema, table, column, timestamp), (mask, percentage))
      })
      .groupBy(_._1)

    val divs = groups
      .map(g => {
        val table = Seq(g._1._1, g._1._2, g._1._3).mkString(".")
        val column = g._1._4
        val ts = g._1._5.toString
        val (masks, percentages) = g._2.map(mp => (mp._2._1, (mp._2._2 * 10000).round.toDouble / 100.0))
          .sortBy(_._2)
          .unzip

        val bar = Bar(percentages, masks)
          .withOrientation(Orientation.Horizontal)

        val yaxis = Axis()
          .withTitle("Mask")
          .withAutomargin(true)

        val xaxis = Axis()
          .withTitle("Percent")
          .withAutomargin(true)

        val lo = Layout()
          .withTitle(s"Top Mask Profiles as of $ts")
          .withAutosize(true)
          .withYaxis(yaxis)
          .withXaxis(xaxis)

        val id = table.replace('.', '-')+'-'+column

        HTMLTemplate(id, Plotly.jsSnippet(id, Seq(bar), lo, Config()))
      }).mkString

    val columns = groups.keySet
      .map(g => {
        val table = Seq(g._1, g._2, g._3).mkString(".")
        val column = g._4

        (table, column)
      })
      .groupBy(_._1)

    val tableOptions = columns.keySet.map(t => {
      s"""<option value="${t.replace('.', '-')}">$t</option>$nl"""
    }).mkString

    val columnSelects = columns.map(t => {
      val start = s"""
         |<select class="column_select" id="${t._1.replace('.', '-')}-columns">
         |  <option disabled selected value>-- select column --</option>
         |""".stripMargin
      val opts = t._2.map(c => s"""  <option value="${c._2}">${c._2}</option>$nl""").mkString
      val end = s"</select>$nl"

      start+opts+end
    }).mkString

    s"""
      |<style>
      |  .mask_profile {
      |    display: none;
      |  }
      |  .column_select {
      |    display: none;
      |  }
      |</style>
      |<script>
      |  $$(function() {
      |    $$("#current_mask_profile_table").change(function() {
      |      const colSel = "#"+$$(this).val()+"-columns"
      |      $$(".column_select").hide();
      |      $$(colSel).show();
      |    });
      |    $$(".column_select").change(function() {
      |      const plotDiv = "#"+$$(this).attr('id').split('-').slice(0,-1).join("-")+"-"+$$(this).val();
      |      console.log(plotDiv);
      |      console.log($$(plotDiv));
      |      $$(".mask_profile").hide();
      |      $$(plotDiv).show();
      |    });
      |  });
      |</script>
      |<select id="current_mask_profile_table">
      |  <option disabled selected value>-- select table --</option>
      |  $tableOptions
      |</select>
      |$columnSelects
      |$divs
      |""".stripMargin
  }
}

object MaskProfile extends MetricObject {
  override def apply(tn: TableName, table: DataFrame): Option[MaskProfile] = {
    val regex_digit = "[0-9]".r
    val regex_char = "[a-zA-Z]".r

    val maskUdf = udf((s: String) => regex_digit.replaceAllIn(regex_char.replaceAllIn(s, "X"), "N"))
    val percentUdf = udf((count: Double, totalCount: Double) => count / totalCount)

    //Snowflake crashes when attempting to process an empty dataframe
    if (table.count() == 0) {
      return None
    }

    val columns = table.schema.fields.map(_.name)
    val values = columns.map(columnName => {

      val total_rows = table.filter(col(columnName).is_not_null).count()

      table.filter(col(columnName).is_not_null)
        .select(maskUdf(col(columnName)).as("mask"))
        .groupBy("mask")
        .count()
        .select(
          col("mask"),
          percentUdf(col("count"), lit(total_rows)).as("percentage")
        )
        .sort(col("percentage").desc)
        .limit(20)
        .select(
          lit(tn.database).as("database"),
          lit(tn.schema).as("schema"),
          lit(tn.table).as("table"),
          lit(columnName).as("column"),
          current_timestamp().as("timestamp"),
          col("mask"),
          col("percentage")
        )


    }).reduceOption(_ union _)

    values match {
      case Some(df) => Some(new MaskProfile(df))
      case None => None
    }
  }

}