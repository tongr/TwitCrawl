package de.hpi.fgis.yql;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class YQLApiJSONTest {
	private static YQLApiJSON api;
	@BeforeClass
	public static void setUp() throws Exception {
		api = new YQLApiJSON();
	}
	@AfterClass
	public static void shutDown() throws Exception {
		api.close();
		api = null;
	}

	@Test
	public void testFormat() {
		assertEquals("json", api.format());
	}
	

	@Test
	public void testQuery() throws IOException {
		String expectedJson = "{\"place\":{\"name\":\"Berlin\"}}";
		DBObject results = api.query("select name from geo.places where woeid = '638242'");
		assertEquals(JSON.parse(expectedJson), results);
	}
	
	@Test
	public void testQueryMeta() throws IOException {
		DBObject meta = api.queryMeta("select name from geo.places where woeid = '638242'", new HashMap<String, String>());
		assertEquals("en-US", ((DBObject)meta.get("query")).get("lang"));
	}
}
