package io.phdata.snowpark.helpers

import io.phdata.snowpark.metrics.SessionConfigBase

class TestFunctions extends SessionConfigBase {

  /**
   * The commented tests are not production tests, just for development
   * Please only uncomment for local development
   */

  /*
  @Test
  def TestMaskProfileReport(): Unit = {
    val metric = new MaskProfile(getSession.table("datarovr.mask_profile"))

    writeReport(new File("report.html"), Seq[Metric](metric))
  }
  
   */

}
