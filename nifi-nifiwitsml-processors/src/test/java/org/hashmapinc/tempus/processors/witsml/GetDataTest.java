package org.hashmapinc.tempus.processors.witsml;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by pc on 16/8/17.
 */
public class GetDataTest {
    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(GetData.class);
    }

    @Test
    public void testProcessor() {

    }
}
