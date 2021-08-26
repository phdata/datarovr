package io.phdata.snowpark.helpers

import io.phdata.snowpark.helpers.functions.companionFromString
import io.phdata.snowpark.metrics.CorrelationMatrix
import org.junit.Assert.assertEquals
import org.junit.Test
import org.scalatest.junit.JUnitSuite

class TestCompanionFromString extends JUnitSuite {

  @Test
  def testCompanionFromString(): Unit = {
    val className = "CorrelationMatrix"

    val comp = companionFromString(className)

    assertEquals(CorrelationMatrix, comp)
  }

}
