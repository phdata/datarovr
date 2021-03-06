package io.phdata.snowpark

import com.snowflake.snowpark.Session
import io.phdata.snowpark.helpers.{Config, ConfigBuilder, TableName}
import io.phdata.snowpark.helpers.functions._
import org.apache.log4j.{Level, Logger}

import java.io.{BufferedWriter, File, FileWriter}

object Main {
  val nl = System.lineSeparator()

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

    println("loading metrics")
    val metrics = config.metrics.map(companionFromString).flatMap(ob => {
      tables.flatMap(t => {
        val df = session.table(t.asMultipartName)

        ob(t, df)
      }).reduceOption(_ union _)
    })

    println("calculating metrics")
    metrics.foreach( m => {
      /**
       * If metric_table_prefix is set, save all metrics to individual tables in snowflake
       */
      config.metric_table_prefix match {
        case Some(prefix) =>
          val tn = TableName(prefix+m.tableSuffix)
          println(s"saving ${m.name} to table $tn")
          appendToSnowflakeTable(tn, m.values)
        case None => ;
      }

      /**
       * If metric_csv_directory is set, save all metrics to individual files in the local directory
       */
      config.metric_csv_directory match {
        case Some(path) =>
          println(s"writing csv file for ${m.name}")
          val p = new File(path)
          if (!p.exists) {
            p.mkdir()
          }
          val f = new File(path+File.separator+m.name)
          if (f.exists) {
            println(s"Output file ${f.getName} already exists! Quitting!")
            sys.exit(1)
          }
          val buffer = new BufferedWriter(new FileWriter(f))
          m.getCSV.foreach(buffer.write)
          buffer.close()
        case None => ;
      }

      /**
       * If metric_dump_table is set, append all metrics to unified table
       */
      config.metric_dump_table match {
        case Some(table) =>
          println(s"saving ${m.name} to dump table $table")
          appendToSnowflakeTable(TableName(table), m.getUnifiedDF)
        case None => ;
      }

      println(s"finished actions for ${m.name}")
    })

    config.html_report match {
      case Some(path) =>
        val p = new File(path)
        if(p.exists()) {
          println("sOutput file ${p.getName} already exists! Quitting!")
          sys.exit(1)
        }
        println(s"writing report file: $path")
        writeReport(p, metrics)
      case None => ;
    }


    println("closing session")
    session.close()
  }
}
