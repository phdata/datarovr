package io.phdata.snowpark.metrics;

import com.snowflake.snowpark.Session;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.scalatest.junit.JUnitSuite;

public class TestingSessionConfigBase extends JUnitSuite {

    public static Session session;

    @BeforeClass
    public static void setupClass() {
        session = Session.builder().configFile("src/test/scala/io/phdata/snowpark/resources/snowflake_config.conf").create();
    }

    @AfterClass
    public static void tearDown() {
        if (session != null) {
            session.close();
        }
    }

    protected Session getSession() { return session; }
}
