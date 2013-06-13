package de.hpi.fgis.json;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class FormatTransformatorTest {
	private static SimpleDateFormat DATE_FORMAT;
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	}
	
	@Test
	public void testFilter() throws ParseException {
		DBObject obj = 
				(DBObject) JSON.parse("{'_id' : 3, 'date' : '2009-09-20', 'user' : {'name' : 'john', 'date' : '1970-01-01'}}");
		FormatTransformator trans = new FormatTransformator(
				new SimpleDateFormat("yyyy-MM-dd"), "date", "user/date");

		DBObject expected = 
				(DBObject) JSON.parse("{'_id' : 3, 'user' : {'name' : 'john'}}");
		
		expected.put("date", DATE_FORMAT.parse("2009-09-20T00:00:00"));
		
		((DBObject) expected.get("user"))
				.put("date", DATE_FORMAT.parse("1970-01-01T00:00:00"));
		
		Assert.assertEquals(expected, trans.transform(obj));
	}

	@Test
	public void testNestedFilter() throws ParseException {

		DBObject obj = (DBObject) JSON
				.parse("{'_id' : 3, 'date' : '2009-09-20', 'users' : [{'name' : 'john', 'date' : '1970-01-01'}, {'name' : 'mike', 'date' : '1989-11-09'}]}");
		FormatTransformator trans = new FormatTransformator(
				new SimpleDateFormat("yyyy-MM-dd"), "date", "users/*/date");

		DBObject expected = 
				(DBObject) JSON.parse("{'_id' : 3, 'users' : [{'name' : 'john'}, {'name' : 'mike'}]}");
		
		expected.put("date", DATE_FORMAT.parse("2009-09-20T00:00:00"));
		
		((DBObject) ((List<?>) expected.get("users")).get(0))
				.put("date", DATE_FORMAT.parse("1970-01-01T00:00:00"));
		
		((DBObject) ((List<?>) expected.get("users")).get(1))
				.put("date", DATE_FORMAT.parse("1989-11-09T00:00:00"));
		
		Assert.assertEquals(expected, trans.transform(obj));
	}

}
