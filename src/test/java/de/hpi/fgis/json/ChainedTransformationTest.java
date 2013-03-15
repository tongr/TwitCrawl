package de.hpi.fgis.json;

import junit.framework.Assert;

import org.junit.Test;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class ChainedTransformationTest {

	@Test
	public void testTransform() {
		ChainedTransformation trans = new ChainedTransformation()
				.addTransformation(new IDFilter())
				.addTransformation(new NullFilter());
		
		
		DBObject obj = (DBObject) JSON
				.parse("{'_id' : 3, 'text' : 'this is a test', 'user' : {'name' : null, '_id' : 12345}}");
		
		Assert.assertEquals(JSON.parse("{'text' : 'this is a test'}"),
				trans.transform(obj));
	}

	@Test
	public void testTransformOrder() {
		ChainedTransformation trans = new ChainedTransformation()
				.addTransformation(new AttributeMultiplier()
						.addTransformation("_id", "old_id"))
				.addTransformation(new IDFilter());
		
		
		DBObject obj = (DBObject) JSON
				.parse("{'_id' : 3, 'text' : 'this is a test', 'user' : {'name' : null, '_id' : 12345}}");
		
		Assert.assertEquals(
				JSON.parse("{'_id' : null, 'text' : 'this is a test', 'old_id' : 3, 'user' : {'name' : null, '_id' : null}}"),
				trans.transform(obj));
		
		
		// add other transformation later -> remove empty objects
		trans.addTransformation(new NullFilter());
				
		obj = (DBObject) JSON
				.parse("{'_id' : 3, 'text' : 'this is a test', 'user' : {'name' : null, '_id' : 12345}}");
		
		Assert.assertEquals(
				JSON.parse("{'text' : 'this is a test', 'old_id' : 3}"),
				trans.transform(obj));
	}

	@Test
	public void testTransformWrongOrder() {
		ChainedTransformation trans = new ChainedTransformation()
				.addTransformation(new IDFilter())
				.addTransformation(new AttributeMultiplier()
						.addTransformation("_id",	"old_id"));
		
		
		DBObject obj = (DBObject) JSON
				.parse("{'_id' : 3, 'text' : 'this is a test', 'user' : {'name' : null, '_id' : 12345}}");
		
		Assert.assertEquals(
				JSON.parse("{'_id' : null, 'text' : 'this is a test', 'user' : {'name' : null, '_id' : null}}"),
				trans.transform(obj));
		
		
		// add other transformation later -> remove empty objects
		trans.addTransformation(new NullFilter());
		
		obj = (DBObject) JSON
				.parse("{'_id' : 3, 'text' : 'this is a test', 'user' : {'name' : null, '_id' : 12345}}");
		
		Assert.assertEquals(
				JSON.parse("{'text' : 'this is a test'}"),
				trans.transform(obj));
	}

}
