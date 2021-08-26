package io.phdata.snowpark

import com.snowflake.snowpark.Session
import io.phdata.snowpark.helpers.{Config, ConfigBuilder, TableName}
import io.phdata.snowpark.helpers.functions._
import org.apache.log4j.{Level, Logger}

import java.io.{BufferedWriter, File, FileWriter}

object Main {

  def main(args: Array[String]) {
    println("retrieving configuration")
    implicit val config: Config = ConfigBuilder.build(args)

    /**
     * Set logging level
     */
    config.log_level match {
      case Some(ll) =>
        val logger = Logger.getLogger("com.snowflake.snowpark")
        logger.setLevel(Level.toLevel(ll))
      case None => ;
    }

    //TODO: validate required config params exist

    println("creating session")
    implicit val session: Session = Session.builder.configs(config.getSnowparkConnectionProperties).create

    println("finding matching tables")
    val tables = getMatchingTables()
    if (tables.size <= 0) {
      println("No matching tables found! Quitting!")
      sys.exit(1)
    }

    val metrics = config.metrics.map(companionFromString).flatMap(ob => {
      val results = tables.flatMap(t => {
        val df = session.table(t.asMultipartName)

        ob(t, df)
      }).reduceOption(_ union _)

      results
    })

    metrics.foreach( r => {
      /**
       * If metric_table_prefix is set, save all metrics to individual tables in snowflake
       */
      config.metric_table_prefix match {
        case Some(prefix) =>
          val tn = TableName(prefix+r.tableSuffix)
          appendToSnowflakeTable(tn, r.values)
        case None => ;
      }

      /**
       * If metric_csv_directory is set, save all metrics to individual files in the local directory
       */
      config.metric_csv_directory match {
        case Some(path) =>
          println(s"writing csv file for ${r.tableSuffix}")
          val p = new File(path)
          if (!p.exists) {
            p.mkdir()
          }
          val f = new File(path+File.separator+r.tableSuffix)
          if (f.exists) {
            println(s"Output file ${f.getName} already exists! Quitting!")
            sys.exit(1)
          }
          val buffer = new BufferedWriter(new FileWriter(f))
          buffer.write(r.getCSVHeader)
          r.getCSVData.foreach(buffer.write)
          buffer.close()
        case None => ;
      }

      /**
       * If metric_dump_table is set, append all metrics to unified table
       */
      config.metric_dump_table match {
        case Some(table) =>
          appendToSnowflakeTable(TableName(table), r.getUnifiedDF)
        case None => ;
      }

      println("finished actions")
    })

    println("closing session")
    session.close()
  }
}
