package io.phdata.snowpark.helpers

import java.io.{File, FileInputStream}

import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._
import scala.collection.mutable

class YamlPropertiesHelper {

  def returnProperties(path: String): mutable.Map[String, Any] = {
    val yamlFile = new FileInputStream(new File(path))
    val yaml = new Yaml()
    yaml.load(yamlFile).asInstanceOf[java.util.Map[String, Any]].asScala
  }
}
