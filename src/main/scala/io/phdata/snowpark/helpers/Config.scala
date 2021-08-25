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
 * @param snowflake_url
 * @param snowflake_db
 * @param snowflake_schema
 * @param snowflake_warehouse
 * @param snowflake_role
 * @param snowflake_user
 * @param snowflake_password
 * @param snowflake_private_key_file
 * @param snowflake_private_key_file_pwd
 * @param snowflake_private_key
 * @param config_file
 * @param schema
 * @param metric_table
 * @param univariate_tests
 * @param multivariate_tests
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
  snowflake_private_key: Option[String] = None,

  config_file: Option[String] = None,
  schema: Option[String] = None,
  metric_table: Option[String] = None,
  univariate_tests: Seq[String] = Seq[String](),
  multivariate_tests: Seq[String] = Seq[String](),
) {

  def describe_snowflake_url: Option[String] = Some("The url to your snowflake account")
  def describe_snowflake_db: Option[String] = Some("Snowflake database to connect to")
  def describe_snowflake_schema: Option[String] = Some("Snowflake schema to connect to")
  def describe_snowflake_warehouse: Option[String] = Some("Snowflake warehouse to connect to")
  def describe_snowflake_role: Option[String] = Some("Snowflake role to connect as")

  def describe_snowflake_user: Option[String] = Some("Snowflake user to connect as")
  def describe_snowflake_password: Option[String] =  Some("Snowflake password for provided user")

  def describe_snowflake_private_key_file: Option[String] = Some("Path to your private key file")
  def describe_snowflake_private_key_file_pwd: Option[String] = Some("Password to your encrypted private key file")
  def describe_snowflake_private_key: Option[String] = None

  def describe_config_file: Option[String] = Some("Path to configuration file")
  def describe_schema: Option[String] = Some("Schema to get tables from")
  def describe_metric_table: Option[String] = Some("Table to store metrics into")
  def describe_univariate_tests: Option[String] = Some("Comma separated list of univariate tests to run")
  def describe_multivariate_tests: Option[String] = Some("Comma separated list of multivariate tests to run")

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