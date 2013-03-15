package de.hpi.fgis.json;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class NullFilterTest {
	private NullFilter filter;

	@Before
	public void setUp() throws Exception {
		this.filter = new NullFilter();
	}

	@Test
	public void testFilterNothing() {
		DBObject obj = (DBObject) JSON.parse("{'_id' : 3, 'text' : 'this is a test', 'user' : {'name' : 'john_doe', 'id' : 12345}}");
		Assert.assertEquals(JSON.parse("{'_id' : 3, 'text' : 'this is a test', 'user' : {'name' : 'john_doe', 'id' : 12345}}"), filter.transform(obj));
	}

	@Test
	public void testFilterEmptyObject() {
		DBObject obj = (DBObject) JSON.parse("{'a' : {}, 'b' : '_B'}");
		Assert.assertEquals(JSON.parse("{'b' : '_B'}"), filter.transform(obj));
	}

	@Test
	public void testFilterNested() {
		DBObject obj = (DBObject) JSON.parse("{'_id' : null, 'text' : 'this is a test', 'user' : {'id' : 12345, 'name' : null}, 'geo' : {'long' : null, 'lat' : null, 'addresses' : [{}, {}]}}");
		Assert.assertEquals(JSON.parse("{'text' : 'this is a test', 'user' : {'id' : 12345}}"), filter.transform(obj));
	}
}
