package de.hpi.fgis.json;

import junit.framework.Assert;

import org.junit.Test;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class AttributeMultiplierTest {
	@Test
	public void testSimple() {
		AttributeMultiplier transformator = new AttributeMultiplier();
		transformator.addTransformation("_id", "old_id")
		.addTransformation("user/name", "username")
		.addTransformation("user/name", "author/name");
		
		DBObject obj = (DBObject) JSON.parse("{'_id' : 3, 'text' : 'this is a test', 'user' : {'name' : 'john_doe', 'id' : 12345}}");
		Assert.assertEquals(JSON.parse("{'_id' : 3, 'text' : 'this is a test', 'user' : {'name' : 'john_doe', 'id' : 12345} , 'username' : 'john_doe', 'author' : {'name' : 'john_doe'}, 'old_id' : 3}"), transformator.transform(obj));
		
		// empty (null) objects should not be copied
		obj = (DBObject) JSON.parse("{'_id' : null, 'text' : 'this is a test', 'user' : {'id' : 12345, 'name' : null}}");
		Assert.assertEquals(JSON.parse("{'_id' : null, 'text' : 'this is a test', 'user' : {'id' : 12345, 'name' : null}}"), transformator.transform(obj));
		
		
	}

	@Test
	public void testAttributeExchange() {
		AttributeMultiplier transformator = new AttributeMultiplier();
		transformator.addTransformation("a/aa", "b/bb")
		.addTransformation("b/bb", "a/aa");
		// test attribute exchange
		DBObject obj = (DBObject) JSON.parse("{'a' : {'aa' : '_A'}, 'b' : {'bb' : '_B'}}");
		Assert.assertEquals(JSON.parse("{'a' : {'aa' : '_B'}, 'b' : {'bb' : '_A'}}"), transformator.transform(obj));
		
	}

	@Test
	public void testNestedArrayMerge() {
		AttributeMultiplier transformator = new AttributeMultiplier();
		transformator.addTransformation("c/*/cc", "d");
		// test array merge
		DBObject obj = (DBObject) JSON.parse("{'c' : [{'cc' : '_A'},{'cc' : '_C'}]}");
		Assert.assertEquals(JSON.parse("{'c' : [{'cc' : '_A'},{'cc' : '_C'}], 'd' : ['_A', '_C']}"),
				transformator.transform(obj));
	}

	@Test
	public void testArrayMerge() {
		AttributeMultiplier transformator = new AttributeMultiplier();
		transformator.addTransformation("c/*", "d");
		// test array merge
		DBObject obj = (DBObject) JSON.parse("{'c' : [{'cc' : '_A'},{'cc' : '_C'}]}");
		Assert.assertEquals(JSON.parse("{'c' : [{'cc' : '_A'},{'cc' : '_C'}], 'd' : [{'cc' : '_A'},{'cc' : '_C'}]}"),
				transformator.transform(obj));
	}


	@Test
	public void testObjectMerge() {
		AttributeMultiplier transformator = new AttributeMultiplier();
		transformator.addTransformation("c/*", "d");
		// test array merge
		DBObject obj = (DBObject) JSON.parse("{'c' : {'test' : {'cc' : {'ccc' : '_A'}}, 'and' : {'cc' : {'ccc' : '_C'}}}}");
		Assert.assertEquals(JSON.parse("{d : [{'cc' : {'ccc' : '_A'}}, {'cc' : {'ccc' : '_C'}}], 'c' : {'test' : {'cc' : {'ccc' : '_A'}}, 'and' : {'cc' : {'ccc' : '_C'}}}}"),
				transformator.transform(obj));
	}


	@Test
	public void testNestedObjectMerge() {
		AttributeMultiplier transformator = new AttributeMultiplier();
		transformator.addTransformation("c/*/cc/ccc", "d");
		// test array merge
		DBObject obj = (DBObject) JSON.parse("{'c' : {'test' : {'cc' : {'ccc' : '_A'}}, 'and' : {'cc' : {'ccc' : '_C'}}}}");
		Assert.assertEquals(JSON.parse("{d : ['_A', '_C'], 'c' : {'test' : {'cc' : {'ccc' : '_A'}}, 'and' : {'cc' : {'ccc' : '_C'}}}}"),
				transformator.transform(obj));
	}




	@Test
	public void testNestedNestedObjectMerge() {
		AttributeMultiplier transformator = new AttributeMultiplier();
		transformator.addTransformation("c/*/*/ccc", "d");
		// test array merge
		DBObject obj = (DBObject) JSON.parse("{'c' : {'test' : {'cc' : {'ccc' : '_A'}}, 'and' : {'cc' : {'ccc' : '_C'}}}}");
		Assert.assertEquals(JSON.parse("{d : [['_A'], ['_C']], 'c' : {'test' : {'cc' : {'ccc' : '_A'}}, 'and' : {'cc' : {'ccc' : '_C'}}}}"),
				transformator.transform(obj));
	}


}
