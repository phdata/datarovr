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
      univariate_tests = Array[String]("one", "two", "three"),
    )


    val m = Map[String, String](
      "snowflake_url" -> "url",
      "univariate_tests" -> "one,two,three",
    )

    val actual = ConfigBuilder.invokePrivate(fromMap(m))

    assertEquals(expected, actual)
  }

  @Test
  def TestFromJConfig(): Unit = {
    val fromJConfig = PrivateMethod[Config]('fromJConfig)

    val expected = Config(
      snowflake_url = Some("url"),
      univariate_tests = Array[String]("one", "two", "three"),
    )

    val actual = ConfigBuilder.invokePrivate(fromJConfig(ConfigFactory.parseResources("test.conf")))

    assertEquals(expected, actual)
  }

  @Test
  def TestBuildArgs(): Unit = {
    val buildArgs = PrivateMethod[Config]('buildArgs)

    val expected = Config(
      snowflake_url = Some("url"),
      univariate_tests = Array[String]("one", "two", "three"),
    )

    val args = Array[String]("--snowflake_url", "url", "--univariate_tests", "one,two,three")

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
      univariate_tests = Seq("two-one", "two-two", "two-three"),
      multivariate_tests = Seq("three-one", "three-two", "three-three"),
    )

    val confs = Seq(
      Config(snowflake_url = Some("one")),
      Config(
        snowflake_db = Some("two"),
        univariate_tests = Seq("two-one", "two-two", "two-three"),
      ),
      Config(
        snowflake_schema = Some("three"),
        multivariate_tests = Seq("three-one", "three-two", "three-three"),
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
      univariate_tests = Seq("one", "two", "three"),
    )

    val confs = Seq(
      Config(snowflake_url = Some("url")),
      Config(
        snowflake_url = Some("foo"),
        univariate_tests = Seq("one", "two", "three")
      ),
      Config(
        snowflake_url = Some("bar"),
        univariate_tests = Seq("foo", "bar", "baz"),
      ),
      Config(snowflake_url = Some("baz")),
    )

    val actual = ConfigBuilder.invokePrivate(layer(confs))

    assertEquals(expected, actual)
  }

}