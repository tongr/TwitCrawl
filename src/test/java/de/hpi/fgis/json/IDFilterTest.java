package de.hpi.fgis.json;

import org.junit.Assert;
import org.junit.Test;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class IDFilterTest {
	@Test
	public void testFilter() {
		DBObject obj = (DBObject) JSON.parse("{'_id' : 3, 'text' : 'this is a test', 'user' : {'name' : 'john', 'id' : 12345}}");
		Assert.assertEquals(JSON.parse("{'_id' : null, 'text' : 'this is a test', 'user' : {'name' : 'john', 'id' : 12345}}"), new IDFilter().transform(obj));
	}
	@Test
	public void testNestedFilter() {
		DBObject obj = (DBObject) JSON.parse("{'_id' : 3, 'text' : 'this is a test', 'user' : {'name' : 'john', 'account' : {'_id' : 12345}}}");
		Assert.assertEquals(JSON.parse("{'_id' : null, 'text' : 'this is a test', 'user' : {'name' : 'john', 'account' : {'_id' : null}}}"), new IDFilter().transform(obj));
	}
}
