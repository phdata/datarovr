package io.phdata.snowpark.helpers
import scala.reflect.runtime.universe._

/**
 * This class represents your application configuration. Attributes added to this class will be automatically
 * populated and discovered by corresponding CLI parameters, environment variables, and config file directives.
 * Currently, those only support "Option[String]" and "Seq[String]" datatypes, if you need a new datatype added
 * to the configuration class, you will have to update the code ConfigBuilder to support it.
 *
 * Every attribute that you create _must_ have a corresponding "describe_" method which is used in the CLI
 * "--help" parameter.
 *
 * @param snowflake_url snowflake account url
 * @param snowflake_db snowflake connection database
 * @param snowflake_schema snowflake connection schema
 * @param snowflake_warehouse snowflake connection warehouse
 * @param snowflake_role snowflake connection role
 * @param snowflake_user snowflake connection user
 * @param snowflake_password snowflake connection password
 * @param snowflake_private_key_file snowflake connection private key file
 * @param snowflake_private_key_file_pwd snowflake connection private key file password
 * @param snowflake_privatekey snowflake connection private key
 * @param config_file path to configuration file
 * @param tables dot separated glob patterns to match _database_._schema_._tables_
 * @param metric_table_prefix string to prefix metrics tables
 * @param metrics seq of metrics to execute
 */
case class Config(
  snowflake_url: Option[String] = None ,
  snowflake_db: Option[String] = None,
  snowflake_schema: Option[String] = None,
  snowflake_warehouse: Option[String] = None,
  snowflake_role: Option[String] = None,

  snowflake_user: Option[String] = None,
  snowflake_password: Option[String] = None,

  snowflake_private_key_file: Option[String] = None,
  snowflake_private_key_file_pwd: Option[String] = None,
  snowflake_privatekey: Option[String] = None,

  config_file: Option[String] = None,
  tables: Option[String] = None,
  metric_table_prefix: Option[String] = None,
  metric_dump_table: Option[String] = None,
  metric_csv_directory: Option[String] = None,
  metrics: Seq[String] = Seq[String](),
  log_level: Option[String] = None,
  html_report: Option[String] = None,
) {

  /**
   * These functions are required for every attribute to generate the --help screen
   * If you don't want an attribute accessible by the CLI params, return None
   */
  def describe_snowflake_url: Option[String] = Some("The url to your snowflake account")
  def describe_snowflake_db: Option[String] = Some("Snowflake database to connect to")
  def describe_snowflake_schema: Option[String] = Some("Snowflake schema to connect to")
  def describe_snowflake_warehouse: Option[String] = Some("Snowflake warehouse to connect to")
  def describe_snowflake_role: Option[String] = Some("Snowflake role to connect as")

  def describe_snowflake_user: Option[String] = Some("Snowflake user to connect as")
  def describe_snowflake_password: Option[String] =  Some("Snowflake password for provided user")

  def describe_snowflake_private_key_file: Option[String] = Some("Path to your private key file")
  def describe_snowflake_private_key_file_pwd: Option[String] = Some("Password to your encrypted private key file")
  def describe_snowflake_privatekey: Option[String] = None

  def describe_config_file: Option[String] = Some("Path to configuration file")
  def describe_tables: Option[String] = Some("Glob matching table names you'd like to profile")
  def describe_metric_table_prefix: Option[String] = Some("Set this prefix to store test results back to snowflake")
  def describe_metric_dump_table: Option[String] = Some("Set this table name to dump unified metric records to a single table")
  def describe_metric_csv_directory: Option[String] = Some("Set this to the directory name to create CSV files in")
  def describe_metrics: Option[String] = Some("Comma separated list of metrics to run")
  def describe_log_level: Option[String] = Some("Set log level")
  def describe_html_report: Option[String] = Some("Set this to the file name of the html output report")

  /**
   * Creates a Map[String,String] that can be passed directly to snowpark's session builder
   * @return a config map for snowpark's session builder
   */
  def getSnowparkConnectionProperties: Map[String, String] = {
    val mconf = runtimeMirror(getClass.getClassLoader).reflect(this)

    Config.fields.map(_.name.toString.trim).filter(_.startsWith("snowflake_")).flatMap(n => {
      val ft = typeOf[Config].decl(TermName(n)).asTerm
      val f = mconf.reflectField(ft)
      val v = f.get.asInstanceOf[Option[String]]
      if (v.isEmpty) {
        None
      } else {
        Some(n.stripPrefix("snowflake_").toUpperCase -> v.get)
      }
    }).toMap
  }

}

object Config {
  def fields: Seq[Symbol] = {
    typeOf[Config].members.filter(!_.isMethod).toSeq
  }
}