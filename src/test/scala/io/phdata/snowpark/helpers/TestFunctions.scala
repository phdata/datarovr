package io.phdata.snowpark.helpers

import io.phdata.snowpark.helpers.functions.writeReport
import io.phdata.snowpark.metrics.{CorrelationMatrix, MaskProfile, Metric, SessionConfigBase}
import org.junit.{Ignore, Test}

import java.io.File

class TestFunctions extends SessionConfigBase {

  //Test ignored because it's just for local development
  @Ignore
  @Test
  def TestReport(): Unit = {
    val mp = new MaskProfile(getSession.table("datarovr.mask_profile"))
    val cm = new CorrelationMatrix(getSession.table("datarovr.correlation_matrix"))

    writeReport(new File("report.html"), Seq(mp, cm))
  }

}
