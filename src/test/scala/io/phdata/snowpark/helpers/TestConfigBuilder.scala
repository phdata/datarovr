package io.phdata.snowpark.helpers

import com.typesafe.config.ConfigFactory
import org.junit.Assert.assertEquals
import org.junit.Test
import org.scalatest.PrivateMethodTester
import org.scalatest.junit.JUnitSuite

class TestConfigBuilder extends JUnitSuite with PrivateMethodTester {

  @Test
  def TestFromMap(): Unit = {
    val fromMap = PrivateMethod[Config]('fromMap)

    val expected = Config(
      snowflake_url = Some("url"),
      metrics = Array[String]("one", "two", "three"),
    )


    val m = Map[String, String](
      "snowflake_url" -> "url",
      "metrics" -> "one,two,three",
    )

    val actual = ConfigBuilder.invokePrivate(fromMap(m))

    assertEquals(expected, actual)
  }

  @Test
  def TestFromMapEmptyMetrics(): Unit = {
    val fromMap = PrivateMethod[Config]('fromMap)

    val expected = Config(
      metrics = Seq.empty[String],
    )

    val m = Map[String, String](
      "metrics" -> "",
    )

    val actual = ConfigBuilder.invokePrivate(fromMap(m))

    assertEquals(expected, actual)
  }

  @Test
  def TestFromJConfig(): Unit = {
    val fromJConfig = PrivateMethod[Config]('fromJConfig)

    val expected = Config(
      snowflake_url = Some("url"),
      metrics = Array[String]("one", "two", "three"),
    )

    val actual = ConfigBuilder.invokePrivate(fromJConfig(ConfigFactory.parseResources("test.conf")))

    assertEquals(expected, actual)
  }

  @Test
  def TestBuildArgs(): Unit = {
    val buildArgs = PrivateMethod[Config]('buildArgs)

    val expected = Config(
      snowflake_url = Some("url"),
      metrics = Array[String]("one", "two", "three"),
    )

    val args = Array[String]("--snowflake_url", "url", "--metrics", "one,two,three")

    val actual = ConfigBuilder.invokePrivate(buildArgs(args))

    assertEquals(expected, actual)
  }

  @Test
  def TestLayerMerge(): Unit = {
    val layer = PrivateMethod[Config]('layer)

    val expected = Config(
      snowflake_url = Some("one"),
      snowflake_db = Some("two"),
      snowflake_schema = Some("three"),
      snowflake_warehouse = Some("four"),
      metrics = Seq("two-one", "two-two", "two-three"),
    )

    val confs = Seq(
      Config(snowflake_url = Some("one")),
      Config(
        snowflake_db = Some("two"),
        metrics = Seq("two-one", "two-two", "two-three"),
      ),
      Config(
        snowflake_schema = Some("three"),
      ),
      Config(snowflake_warehouse = Some("four")),
    )

    val actual = ConfigBuilder.invokePrivate(layer(confs))

    assertEquals(expected, actual)
  }

  @Test
  def TestLayerNoOverwrite(): Unit = {
    val layer = PrivateMethod[Config]('layer)

    val expected = Config(
      snowflake_url = Some("url"),
      metrics = Seq("one", "two", "three"),
    )

    val confs = Seq(
      Config(snowflake_url = Some("url")),
      Config(
        snowflake_url = Some("foo"),
        metrics = Seq("one", "two", "three")
      ),
      Config(
        snowflake_url = Some("bar"),
        metrics = Seq("foo", "bar", "baz"),
      ),
      Config(snowflake_url = Some("baz")),
    )

    val actual = ConfigBuilder.invokePrivate(layer(confs))

    assertEquals(expected, actual)
  }

}