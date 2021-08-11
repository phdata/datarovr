package io.phdata.snowpark.metrics;

import com.snowflake.snowpark.Session;
import io.phdata.snowpark.helpers.EnvPropertyHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scalatest.junit.JUnitSuite;

public class SessionConfigBase extends JUnitSuite {

    public static Session session;

    @BeforeClass
    public static void setupClass() {
        session = Session.builder().configs((new EnvPropertyHelper()).getSnowflakeConnectionProperties()).create();
    }

    @AfterClass
    public static void tearDown() {
        if (session != null) {
            session.close();
        }
    }

    @Test
    public void dummy() { Assert.assertTrue(true);}


    protected Session getSession() { return session; }
}
