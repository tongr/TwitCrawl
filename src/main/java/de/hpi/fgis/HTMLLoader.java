package de.hpi.fgis;

import java.io.IOException;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import de.hpi.fgis.html.WebPageExtractor;

public class HTMLLoader {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) {
		if(args.length<=0) {
			throw new IllegalArgumentException("Please specify an URL o load!");
		} 
		//MongoDBObjectManager pageMan = new MongoDBObjectManager("webpages", false);
		WebPageExtractor extractor = new WebPageExtractor();
		for(String url : args) {
			System.out.println("extracting data from " + url);
			
			DBObject data = extractor.transform(new BasicDBObject("url", url));
			System.out.println(JSON.serialize(data));
			// TODO add data to mongo
		}
	}

}
