package io.phdata.snowpark.helpers

import scala.collection.mutable.ArrayBuffer

case class TableName(database: String, schema: String, table: String) {
  /**
   * @return the fully quoted and fully qualified table name as a string
   */
  override def toString: String = {
    q(database)+'.'+q(schema)+'.'+q(table)
  }

  /**
   * Convenience function for passing a table name to some snowflake functions
   * @return a Seq representing the fully qualifed table name
   */
  def asMultipartName: Seq[String] = {
    Seq(q(database), q(schema), q(table))
  }

  private def q(s: String): String = {
    '"'+s+'"'
  }
}

object TableName {

  /**
   * Parses a string into a table name
   * @param pattern a string representing a relative or fully qualified table
   * @param conf implicit config
   * @return a parsed TableName
   */
  def apply(pattern: String)(implicit conf: Config): TableName = {
    val ab: ArrayBuffer[String] = ArrayBuffer()

    val sb: StringBuilder = new StringBuilder()
    var inQuote = false
    var i = 0
    while(i < pattern.length) {
      pattern(i) match {

        case '\\' =>
          if (i+1 >= pattern.length) {
            sb += '\\'
          } else {
            pattern(i+1) match {
              case '.' => sb += '.'
              case '"' => sb += '"'
              case x => sb ++= Seq('\\', x)
            }
            i += 1
          }
        case '"' =>
          inQuote = !inQuote
        case '.' if !inQuote =>
          ab += sb.result
          sb.clear()
        case x =>
          sb += { if (inQuote) x else x.toUpper }
      }
      i += 1
    }
    ab += sb.result

    ab.length match {
      case 3 =>
        new TableName(ab(0), ab(1), ab(2))
      case 2 => ;
        new TableName(relativeDB, ab(0), ab(1))
      case 1 => ;
        new TableName(relativeDB, relativeSchema, ab(0))
      case _ =>
        throw new Exception("invalid table name")
    }
  }

  private def relativeDB(implicit conf: Config): String = {
    val db = conf.snowflake_db.get
    if (db.startsWith("\"") && db.endsWith("\"")) {
      db
    } else {
      db.toUpperCase
    }
  }

  private def relativeSchema(implicit conf: Config): String = {
    val schema = conf.snowflake_schema.get
    if (schema.startsWith("\"") && schema.endsWith("\"")) {
      schema
    } else {
      schema.toUpperCase
    }
  }
}
