package de.hpi.fgis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.mongodb.DBObject;

import de.hpi.fgis.concurrency.APIAccessRateLimitGuard.RateLimitedTask;
import de.hpi.fgis.concurrency.AsyncResultHandler;
import de.hpi.fgis.twitter.TwitterDumpFileReader;
import de.hpi.fgis.util.FileUtil;
import de.hpi.fgis.util.ProgressReport;
import de.hpi.fgis.yql.YQLAccessRateLimitGuard;
import de.hpi.fgis.yql.YQLCrawler;
import de.hpi.fgis.yql.YQLCrawler.CrawlingResults;

/**
 * Basic executor to parse a tweet file and crawl all contained urls via YQL
 * @author tongr
 */
public class YQLDumpFileCrawler {
	public static void main(String[] args) {
		String folder;
		if(args.length<=0) {
			folder = "./";
		} else {
			folder = args[0];
		}
		
		new YQLDumpFileCrawler().parse(new FileUtil().scan(folder, "tweets_w_links_n_htags_.*\\.stream", false).toArray(new String[0]));
	}
	private boolean finished = false;
	private final int chunkSize = 100;
	private void parse(String... files) {
		System.out.println(new Date().toString());
		
		final YQLAccessRateLimitGuard guard = YQLAccessRateLimitGuard.getInstance();
		final YQLCrawler crawler = new YQLCrawler();
		final Queue<String> urls = new LinkedList<>();
		final ProgressReport rpt = new ProgressReport("Crawling urls from tweets ...").setUnit("urls").setReport(2500);
		RateLimitedTask task = new RateLimitedTask() {
			@Override
			public void run() {
				try {
					final ArrayList<String> toBeCrawled = new ArrayList<>(chunkSize);
					synchronized (urls) {
						while(toBeCrawled.size()<chunkSize && urls.size()>0) {
							toBeCrawled.add(urls.poll());
						}
					}
					if(toBeCrawled.size()>0) {
						crawler.crawlAsync(toBeCrawled, new AsyncResultHandler<CrawlingResults>() {
							
							@Override
							public void onThrowable(Throwable t) {
								t.printStackTrace();
							}
							
							@Override
							public void onCompleted(CrawlingResults data) {
								System.out.println("contents of " + data.contents().size() + " out of " + toBeCrawled.size() + " urls extracted ...");
								rpt.inc(toBeCrawled.size());
							}
						});
					} else {
						System.out.println("contents of " + 0 + " out of " + toBeCrawled.size() + " urls extracted ...");
					}
				} catch (IOException e1) {
					e1.printStackTrace();
					return;
				}
			}
			
			@Override
			public boolean repeat() {
				if(finished) {
					rpt.finish();
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
			TwitterDumpFileReader reader = new TwitterDumpFileReader(file).setShowProgress(false);
			
			for(DBObject tweet : reader) {
				if(tweet.containsField("urls") && tweet.get("urls") instanceof List) {
					int urlCount;
					synchronized (urls) {
						@SuppressWarnings("unchecked")
						List<String> listOfUrls = (List<String>)tweet.get("urls");
						urls.addAll(listOfUrls);
						urlCount = urls.size();
					}
					while(urlCount>chunkSize*2) {
						try {
							synchronized (reader) {
								reader.wait(200);
							}
						} catch (InterruptedException e) {
							// ignore
						}
						synchronized (urls) {
							urlCount = urls.size();
						}
					}
				}
			}
		}
		finished = true;
		System.out.println(new Date().toString());
	}
}
