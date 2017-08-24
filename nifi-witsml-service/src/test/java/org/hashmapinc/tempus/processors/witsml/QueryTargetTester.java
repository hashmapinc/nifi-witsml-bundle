package org.hashmapinc.tempus.processors.witsml;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class QueryTargetTester {

    @Test
    public void TestQueryTargetCreatorWellbore(){
        List<String> objects = new ArrayList<>();
        objects.add("well");
        QueryTarget target = QueryTarget.parseURI("/testWell(123)/testWellbore(456)", objects);
        assertEquals(target.getQueryLevel(), QueryLevel.Wellbore);
        assertArrayEquals(target.getObjectsToQuery().toArray(), objects.toArray());
        assertEquals(target.getWell().getName(), "testWell");
        assertEquals(target.getWell().getId(), "123");
        assertEquals(target.getWell().getType(), "well");
        assertEquals(target.getWellbore().getName(), "testWellbore");
        assertEquals(target.getWellbore().getId(), "456");
        assertEquals(target.getWellbore().getType(), "wellbore");

    }

    @Test
    public void TestQueryTargetCreatorServer(){
        List<String> objects = new ArrayList<>();
        objects.add("well");
        QueryTarget target = QueryTarget.parseURI("/", objects);
        assertEquals(target.getQueryLevel(), QueryLevel.Server);
        assertArrayEquals(target.getObjectsToQuery().toArray(), objects.toArray());
        assertEquals(target.getWell(), null);
        assertEquals(target.getWellbore(), null);
    }

    @Test
    public void TestQueryTargetCreatorWell(){
        List<String> objects = new ArrayList<>();
        objects.add("well");
        QueryTarget target = QueryTarget.parseURI("/testWell(789)", objects);
        assertEquals(QueryLevel.Well, target.getQueryLevel());
        assertArrayEquals(objects.toArray(), target.getObjectsToQuery().toArray());
        assertEquals("testWell", target.getWell().getName());
        assertEquals("789", target.getWell().getId());
        assertEquals("well", target.getWell().getType());
        assertEquals(null, target.getWellbore());
    }
}
