package io.phdata.snowpark.helpers
import com.typesafe.config.{Config => JConfig, ConfigFactory}
import scopt.OParser
import scala.reflect.runtime.universe._

import java.io.File

object ConfigBuilder {
  /**
   * Builds a Config object representing the interpreted configuration based on layering the
   * configuration avenues with the following preference heirarchy:
   * Command line arguments -> Environment variables -> Configruation File -> System Default
   * Interpreted Parameters are selected from the first non-empty avenue.
   * @param args They command line arguments typically passed into the main() function
   * @return a merged config object
   */
  def build(args: Array[String]): Config = {
    val aConfig = buildArgs(args)
    val eConfig = buildEnv()
    val dConfig = buildDefault()
    val path = aConfig.config_file.getOrElse(
      eConfig.config_file.getOrElse(
        dConfig.config_file.getOrElse("")
      )
    )
    val file = getFile(path)
    val fConfig = if (file.nonEmpty) buildFile(file.get) else Config()

    layer(Seq(aConfig, eConfig, fConfig, dConfig))
  }

  /**
   * This method will iterate over the passed sequence of configs, and return a merged config
   * where the first non-empty attribute is selected from each one
   * @param confs the sequence of configs to layer
   * @return a merged config object
   */
  private def layer(confs: Seq[Config]): Config = {
    val conf = Config()
    val mirror = runtimeMirror(getClass.getClassLoader)
    val mconf = mirror.reflect(conf)

    Config.fields.map(m => m.name.toString.trim -> m.typeSignature).foreach { case (name, tpe) =>
      val ft = typeOf[Config].decl(TermName(name)).asTerm
      val f = mconf.reflectField(ft)

      tpe.toString match {
        case "Option[String]" =>
          f.set({
            confs.map(c => {
              val mc = mirror.reflect(c)
              val tf = mc.reflectField(ft)

              tf.get.asInstanceOf[Option[String]]
            }).find(_.nonEmpty).flatten
          })
        case "Seq[String]" =>
          f.set({
            confs.map(c => {
              val mc = mirror.reflect(c)
              val tf = mc.reflectField(ft)

              tf.get.asInstanceOf[Seq[String]]
            }).find(_.nonEmpty).getOrElse(Seq[String]())
          })
      }
    }

    conf
  }

  /**
   * Builds a default configuration by parsing the embedded resource at "default.conf"
   * @return a Config object housing all system defaults
   */
  private def buildDefault(): Config = {
    fromJConfig(ConfigFactory.parseResources("default.conf"))
  }

  /**
   * Returns an Option[File] object based on a path only if the file exists otherwise None
   * @param path path to the configuration file
   * @return
   */
  private def getFile(path: String): Option[File] = {
    val file = new File(path)
    if (file.exists) {
      Some(file)
    } else {
      None
    }
  }

  /**
   * Generates a Config object representing the parsed File that was passed
   * @param file File object representing the configuration file
   * @return a Config object from the parsed file
   */
  private def buildFile(file: File): Config = {
      fromJConfig(ConfigFactory.parseFile(file))
  }

  /**
   * Generates a config object from a typesafe.Config object (locally known as a JConfig)
   * @param jConf the typesafe.Config object to generate our config from
   * @return the config object that we generated
   */
  private def fromJConfig(jConf: JConfig): Config = {
    val map = Config.fields.map(_.name.toString.trim).flatMap(n => {
      if (jConf.hasPath(n)) {
        Some(n -> jConf.getString(n))
      } else {
        None
      }
    }).toMap

    fromMap(map)
  }

  /**
   * Generates a config object by parsing environment variables that begin with DR_
   * @return a config object based on the system environment
   */
  private def buildEnv(): Config = {
    val map: Map[String, String] = Config.fields.map(_.name.toString.trim).map(n => {
      n -> sys.env.getOrElse("DR_"+n.toUpperCase, "")
    }).toMap

    fromMap(map)
  }

  /**
   * Generates a config object based on the passed command line arguments
   * @param args the Array[String] that is usually passed to main()
   * @return a config object represented by the command line args
   */
  private def buildArgs(args: Array[String]): Config = {
    val conf = Config()
    val mirror = runtimeMirror(getClass.getClassLoader)
    val mconf = mirror.reflect(conf)

    val builder = OParser.builder[Config]
    var parser = {
      OParser.sequence(
        builder.programName("java -jar datarovr.jar"),
        builder.head("DataRovr", "<version>"),
      )
    }

    Config.fields.map(m => (m.name.toString.trim, m.typeSignature)).foreach { case (name, tpe) =>
      val ft = typeOf[Config].decl(TermName(name)).asTerm
      val dm = typeOf[Config].decl(TermName("describe_"+name)).asMethod
      val d = mconf.reflectMethod(dm)
      d().asInstanceOf[Option[String]] match {
        case Some(desc) =>
          tpe.toString match {
            case "Option[String]" =>
              parser = parser ++ OParser.sequence(
                builder.opt[String](name)
                  .text(desc)
                  .action((x, c) => {
                    val mc = mirror.reflect(c)
                    val f = mc.reflectField(ft)
                    f.set(Some(x))
                    c
                  })
              )
            case "Seq[String]" =>
              parser = parser ++ OParser.sequence(
                builder.opt[Seq[String]](name)
                  .text(desc)
                  .action((x, c) => {
                    val mc = mirror.reflect(c)
                    val f = mc.reflectField(ft)
                    f.set(x)
                    c
                  })
              )
            case _ => ;
          }
        case None => ;
      }
    }

    parser = parser ++ OParser.sequence(builder.help('h', "help").text("prints this usage text"))

    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        config
      case _ =>
        //bad args
        Config()
    }
  }

  /**
   * Generates a config object from a Map[String, String]
   * @param map configuration parameters
   * @return a config object created from the passed parameters
   */
  private def fromMap(map: Map[String, String]): Config = {
    val conf = Config()
    val mconf = runtimeMirror(getClass.getClassLoader).reflect(conf)

    Config.fields.map(m => (m.name.toString.trim, m.typeSignature)).foreach{ case (name, tpe) =>
      val ft = typeOf[Config].decl(TermName(name)).asTerm
      val f = mconf.reflectField(ft)
      tpe.toString match {
        case "Option[String]" =>
          map.get(name) match {
            case Some(v) if v == "" =>
              f.set(None)
            case Some(v) =>
              f.set(Some(v))
            case None => ;
          }
        case "Seq[String]" =>
          map.get(name) match {
            case Some(v) if v == "" =>
              f.set(Seq.empty[String])
            case Some(v) =>
              f.set(v.split(',').toSeq)
            case None => ;
          }
        case _ => ;
      }
    }

    conf
  }
}
