package io.phdata.snowpark.helpers

import io.phdata.snowpark.helpers.functions.writeReport
import io.phdata.snowpark.metrics.{CorrelationMatrix, MaskProfile, Metric, SessionConfigBase}
import org.junit.Test

import java.io.File

class TestFunctions extends SessionConfigBase {

  /**
   * The commented tests are not production tests, just for development
   * Please only uncomment for local development
   */

  @Test
  def TestReport(): Unit = {
    val mp = new MaskProfile(getSession.table("datarovr.mask_profile"))
    val cm = new CorrelationMatrix(getSession.table("datarovr.correlation_matrix"))

    writeReport(new File("report.html"), Seq(mp, cm))
  }

}
