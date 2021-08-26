package io.phdata.snowpark.metrics

import com.snowflake.snowpark.Session
import io.phdata.snowpark.helpers.{Config, ConfigBuilder}
import io.phdata.snowpark.metrics.SessionConfigBase.session
import org.junit.AfterClass
import org.junit.BeforeClass


object SessionConfigBase {
  var config: Option[Config] = None
  var session: Option[Session] = None

  @BeforeClass
  def setupClass(): Unit = {
    config = Some(ConfigBuilder.build(Array()))
    session = Some(Session.builder.configs(config.get.getSnowparkConnectionProperties).create)
  }

  @AfterClass
  def tearDown(): Unit = {
   session.foreach(_.close())
  }

}

class SessionConfigBase {
  protected def getSession: Session = session.get
}