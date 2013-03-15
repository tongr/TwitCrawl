package de.hpi.fgis;

import java.util.Date;

import de.hpi.fgis.database.mongodb.MongoDBObjectManager;
import de.hpi.fgis.twitter.TwitterDumpFileReader;
import de.hpi.fgis.util.FileUtil;

/**
 * @author tongr
 */
public class DumpFileParser {
	public static void main(String[] args) {
		String folder;
		if(args.length<=0) {
			folder = "./";
		} else {
			folder = args[0];
		}
		
		parse(new FileUtil().scan(folder, "tweets_w_links_n_htags_.*\\.stream", false).toArray(new String[0]));
	}
	
	@SuppressWarnings("deprecation")
	private static void parse(String... files) {
		System.out.println(new Date().toGMTString());
		MongoDBObjectManager tweetsMan = new MongoDBObjectManager("tweets", false);
		for(String file : files) {
			System.out.print("parsing tweets of: ");
			System.out.println(file);
			TwitterDumpFileReader reader = new TwitterDumpFileReader(file); 
			tweetsMan.store(reader);
			tweetsMan.commit();
		}
		tweetsMan.close();
		System.out.println(new Date().toGMTString());
	}
}
