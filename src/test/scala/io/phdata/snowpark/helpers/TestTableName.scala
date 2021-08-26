package io.phdata.snowpark.helpers

import org.junit.Assert.assertEquals
import org.junit.Test
import org.scalatest.PrivateMethodTester
import org.scalatest.junit.JUnitSuite

class TestTableName extends JUnitSuite with PrivateMethodTester {

  implicit val conf: Config = Config(snowflake_db=Some("one"), snowflake_schema=Some("two"))

  @Test
  def TestApplyStandard(): Unit = {
    assertEquals(new TableName("ONE", "TWO", "THREE"), TableName("one.two.three"))
  }

  @Test
  def TestApplyMissingDB(): Unit = {
    assertEquals(new TableName("ONE", "TWO", "THREE"), TableName("two.three"))
  }

  @Test
  def TestApplyMissingDBAndSchema(): Unit = {
    assertEquals(new TableName("ONE", "TWO", "THREE"), TableName("three"))
  }

  @Test
  def TestApplyLowercase(): Unit = {
    assertEquals(new TableName("one", "two", "three"), TableName("\"one\".\"two\".\"three\""))
  }

  @Test
  def TestApplyEscape(): Unit = {
    assertEquals(new TableName("one", "TW.O", "thre\"e"), TableName("\"one\".tw\\.o.\"thre\\\"e\""))
  }

  @Test
  def TestApplyEscapeStar(): Unit = {
    assertEquals(new TableName("one", "two", "t\\*"), TableName("\"one\".\"two\".\"t\\*\""))
  }
}
