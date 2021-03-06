package de.hpi.fgis.yql;

import java.io.InputStream;
import java.util.logging.Level;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;

/**
 * this class provides the possibility to access the <a
 * href="http://developer.yahoo.com/yql/">Yahoo! Query Language (YQL)</a>
 * web-interface using the JSON serialization.
 * 
 * @author tonigr
 * 
 */
public class YQLApiJSON extends YQLApi {
	/**
	 * create a new YQL API access instance that uses JSON serialization and the public YQL endpoint
	 */
	public YQLApiJSON() {
		super();
	}

	/*
	 * (non-Javadoc)
	 * @see de.hpi.fgis.yql.YQLApi#format()
	 */
	@Override
	protected String format() {
		return "json";
	}

	/*
	 * (non-Javadoc)
	 * @see de.hpi.fgis.yql.YQLApi#parse(java.io.InputStream)
	 */
	@Override
	protected DBObject parse(InputStream jsonIn) throws DeserializationException {
		String data = convertStreamToString(jsonIn, "UTF-8");
		try {
			if(data==null || !data.matches("(?s)(?m)^[\\s]*\\{.*")) {
				// log the erroneous data
				LOG.info("Ignoring illegal serialization format (expecting proper JSON): " + data);
				
				throw new DeserializationException("Unable to parse serialization format (expecting proper JSON): \n" + data);
			}
			return (DBObject) JSON.parse(data);
		} catch (JSONParseException e) {
			LOG.log(Level.INFO, "Unable to parse JSON string: " + data, e);
			throw new DeserializationException("Unable to parse serialization format (expecting proper JSON): \n" + data, e);
			
		}
		
	}
}
