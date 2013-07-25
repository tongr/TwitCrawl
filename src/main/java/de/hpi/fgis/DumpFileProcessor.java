package de.hpi.fgis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import com.mongodb.DBObject;

import de.hpi.fgis.concurrency.APIAccessRateLimitGuard.RateLimitedTask;
import de.hpi.fgis.html.ContentExtractor;
import de.hpi.fgis.twitter.TwitterDumpFileReader;
import de.hpi.fgis.util.FileUtil;
import de.hpi.fgis.yql.YQLAccessRateLimitGuard;
import de.hpi.fgis.yql.YQLCrawler;

/**
 * @author tongr
 */
public class DumpFileProcessor {
	public static void main(String[] args) {
		String folder;
		if(args.length<=0) {
			folder = "./";
		} else {
			folder = args[0];
		}
		
		new DumpFileProcessor().parse(new FileUtil().scan(folder, "tweets_w_links_n_htags_.*\\.stream", false).toArray(new String[0]));
	}
	private boolean finished = false;
	private void parse(String... files) {
		System.out.println(new Date().toString());
//		MongoDBObjectManager tweetsMan = new MongoDBObjectManager("hashtags", false);
//		MongoDBObjectManager contentMan = new MongoDBObjectManager("hashtags", false);
		
		
		final YQLAccessRateLimitGuard guard = YQLAccessRateLimitGuard.getInstance();
		final YQLCrawler crawler = new YQLCrawler();
		final Queue<String> urls = new LinkedList<>();
		RateLimitedTask task = new RateLimitedTask() {
			@Override
			public void run() {
				Map<String, String> redirects = new HashMap<>();
				Map<String, String> content;
				try {
					ArrayList<String> toBeCrawled = new ArrayList<>(100);
					synchronized (urls) {
						while(toBeCrawled.size()<100 && urls.size()>0) {
							toBeCrawled.add(urls.poll());
						}
					}
					content = crawler.crawl(Arrays.asList("http://bit.ly/13M0qc8","http://ow.ly/nf5Hv", "http://kbstroy.ru/img/mim.php?p=kdw36dfsi1"), redirects);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					return;
				}
				
				
				System.out.println("contents:");
				for(Entry<String, String> e : content.entrySet()) {
					System.out.println(e.getKey());
					System.out.print(" --> ");
					System.out.println(ContentExtractor.CanolaExtractor.extractText(e.getValue()));
				}
				
				System.out.println("redirects:");
				for(Entry<String, String> e : redirects.entrySet()) {
					System.out.print(e.getKey());
					System.out.print(" --> ");
					System.out.println(e.getValue());
				} 
			}
			
			@Override
			public boolean repeat() {
				if(finished) {
					guard.close();
					return false;
				}
				return true;
			}
		};
		guard.add(task);
		
		for(String file : files) {
			System.out.print("parsing tweets of: ");
			System.out.println(file);
			TwitterDumpFileReader reader = new TwitterDumpFileReader(file);
			
			for(DBObject tweet : reader) {
				if(tweet.containsField("urls") && tweet.get("urls") instanceof List) {
					synchronized (urls) {
						@SuppressWarnings("unchecked")
						List<String> listOfUrls = (List<String>)tweet.get("urls");
						urls.addAll(listOfUrls);
					}
				}
			}
		}
		finished = true;
		System.out.println(new Date().toString());
	}
}
