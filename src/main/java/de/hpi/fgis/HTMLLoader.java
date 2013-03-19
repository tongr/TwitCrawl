package de.hpi.fgis;

import java.io.IOException;
import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import de.hpi.fgis.database.mongodb.MongoDBObjectManager;
import de.hpi.fgis.html.WebPageExtractor;
import de.hpi.fgis.json.ChainedTransformation;
import de.hpi.fgis.json.ITransformation;
import de.hpi.fgis.json.NullFilter;

public class HTMLLoader {

	/**
	 * @param args
	 * @throws IOException 
	 */
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		if(args.length<=0) {
			throw new IllegalArgumentException("Please specify one or more URL(s) to be loaded!");
		}
		System.out.println(new Date().toGMTString());
		MongoDBObjectManager pageMan = new MongoDBObjectManager("webpages", false);
		
		// extract web pages and ignore empty attribute values
		ITransformation extractor = new ChainedTransformation()
			.addTransformation(new WebPageExtractor())
			.addTransformation(new NullFilter());
		for(String url : args) {
			System.out.println("extracting data from " + url);
			
			DBObject pageData = extractor.transform(new BasicDBObject("url", url));
			// add data to mongo
			pageMan.store(pageData);
			pageMan.commit();
		}
		pageMan.close();
		System.out.println(new Date().toGMTString());
	}

}
