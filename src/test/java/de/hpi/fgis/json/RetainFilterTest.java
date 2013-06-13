package de.hpi.fgis.json;

import org.junit.Assert;
import org.junit.Test;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class RetainFilterTest {
	

	@Test
	public void testFilter() {
		RetainFilter filter = new RetainFilter("text", "user/first_name", "user/last_name");
			
		DBObject obj = (DBObject) JSON.parse("{'_id' : 3, 'text' : 'this is a test', 'user' : {'first_name' : 'john', 'last_name' : 'doe', 'id' : 12345}}");
		Assert.assertEquals(JSON.parse("{'_id' : null, 'text': 'this is a test', 'user' : {'first_name' : 'john', 'last_name' : 'doe', 'id' : null}}"), filter.transform(obj));
		
		// empty user object should not be removed
		obj = (DBObject) JSON.parse("{'_id' : 3, 'text' : 'this is a test', 'user' : {'id' : 12345, 'first_name' : null}}");
		Assert.assertEquals(JSON.parse("{'_id' : null, 'text' : 'this is a test', 'user' : {'id' : null, 'first_name' : null}}"), filter.transform(obj));
	}


	@Test
	public void testRetainAllListEntries() {
		RetainFilter filter = new RetainFilter("text", "users/*/first_name", "users/*/last_name");
		DBObject obj = (DBObject) JSON.parse("{'_id' : 3, 'text' : 'this is a test', 'users' : [{'first_name' : 'john', 'last_name' : 'doe', 'id' : 12345}, {'first_name' : 'mike'}]}");
		Assert.assertEquals(JSON.parse("{'_id' : null, 'text': 'this is a test', 'users' : [{'first_name' : 'john', 'last_name' : 'doe', 'id' : null}, {'first_name' : 'mike'}]}"), filter.transform(obj));
		
		// empty user object should not be removed
		obj = (DBObject) JSON.parse("{'_id' : 3, 'text' : 'this is a test', 'users' : ['john', 'mike']}}");
		Assert.assertEquals(JSON.parse("{'_id' : null, 'text' : 'this is a test', 'users' : ['john', 'mike']}"), filter.transform(obj));
	}


	@Test
	public void testRetainAllObjectValues() {
		RetainFilter filter = new RetainFilter("text", "user/*/city", "user/*/0");
		DBObject obj = (DBObject) JSON.parse("{'_id' : 3, 'text' : 'this is a test', 'user' : {'first_name' : 'john', 'last_name' : 'doe', 'id' : 12345, 'degree' : ['dr', 'msc'], address : {'street': 'dunno', 'city' : 'N/A'}}}");
		Assert.assertEquals(JSON.parse("{'_id' : null, 'text': 'this is a test', 'user' :{'first_name' : 'john', 'last_name' : 'doe', 'id' : 12345, 'degree' : [ 'dr', null ], address : {'street': null, 'city' : 'N/A'}}}"), filter.transform(obj));
		
		filter = new RetainFilter("text", "user/*");
		obj = (DBObject) JSON.parse("{'_id' : 3, 'text' : 'this is a test', 'user' : {'first_name' : 'john', 'last_name' : 'doe', 'id' : 12345, 'degree' : ['dr', 'msc'], address : {'street': 'dunno', 'city' : 'N/A'}}}");
		Assert.assertEquals(JSON.parse("{'_id' : null, 'text': 'this is a test', 'user' :{'first_name' : 'john', 'last_name' : 'doe', 'id' : 12345, 'degree' : [ 'dr', 'msc' ], address : {'street': 'dunno', 'city' : 'N/A'}}}"), filter.transform(obj));
	
	}
}
