package de.hpi.fgis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import de.hpi.fgis.concurrency.APIAccessRateLimitGuard.RateLimitedTask;
import de.hpi.fgis.concurrency.AsyncResultHandler;
import de.hpi.fgis.database.mongodb.CachedMongoDBObjectManager;
import de.hpi.fgis.database.mongodb.MongoDBObjectManager;
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
			//folder = "./";
			throw new IllegalArgumentException("Please specify an input folder!");
		} else {
			folder = args[0];
		}
		
		new YQLDumpFileCrawler().parse(new FileUtil().scan(folder, "tweets_w_links_n_htags_.*\\.stream", false).toArray(new String[0]));
	}
	private boolean finished = false;
	private final int chunkSize = 100;
	private final CachedMongoDBObjectManager redirectMan = new CachedMongoDBObjectManager(new MongoDBObjectManager("redirects", false), "from", 1000000, true);
	private final MongoDBObjectManager webpageSink = new MongoDBObjectManager("webpages", false);
	private final MongoDBObjectManager alignmentSink = new MongoDBObjectManager("alignments", false);
	private final MongoDBObjectManager tweetSink = new MongoDBObjectManager("tweets", false);
	
	private void parse(String... files) {
		System.out.println(new Date().toString());
		
		
		final YQLAccessRateLimitGuard guard = YQLAccessRateLimitGuard.getInstance();
		final YQLCrawler crawler = new YQLCrawler();
		
		final Queue<AlignmentCandidate> alignmentCandidates = new LinkedList<>();
		final ProgressReport rpt = new ProgressReport("Crawling urls from tweets ...").setUnit("tweets").setReport(2500);
		RateLimitedTask task = new RateLimitedTask() {
			@Override
			public void run() {
				try {
					final ArrayList<String> toBeCrawled = new ArrayList<>(chunkSize*2);
					final ArrayList<AlignmentCandidate> currentAlignments = new ArrayList<>(chunkSize);
					final HashMap<String, String> cachedRedirects = new HashMap<>();
					
					synchronized (alignmentCandidates) {
						while(toBeCrawled.size()<chunkSize && alignmentCandidates.size()>0) {
							AlignmentCandidate candidate = alignmentCandidates.poll();
							currentAlignments.add(candidate);
							
							// TODO move out of the synchronized block?
							for(String url : candidate.originalUrls()) {
								DBObject redirect = redirectMan.findOne(url);
								
								if(redirect==null) {
									toBeCrawled.add(url);
								} else {
									cachedRedirects.put(url, (String) redirect.get("to")); 
								}
							}
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
								if(data==null) {
									return;
								}
								try {
									ArrayList<DBObject> items = new ArrayList<>(data.redirects().size());

									// store redirects
									for(Entry<String, String> e : data.redirects().entrySet()) {
										DBObject newItem = new BasicDBObject(2);
										newItem.put("from", e.getKey());
										newItem.put("to", e.getValue());
										items.add(newItem);
									}
									redirectMan.store(items);

									
									// store web content
									items = new ArrayList<>(data.contents().size());
									for(Entry<String, String> e : data.contents().entrySet()) {
										DBObject newItem = new BasicDBObject(2);
										newItem.put("url", e.getKey());
										newItem.put("content", e.getValue());
										items.add(newItem);
									}
									webpageSink.store(items);
									
									final int maxBulkSize = chunkSize*10;
									// store alignments
									items = new ArrayList<>(maxBulkSize);
									for(AlignmentCandidate alignment : currentAlignments) {
										HashSet<String> actualUrls = new HashSet<>(alignment.originalUrls().size()); 
										for(String origUrl : alignment.originalUrls()) {
											if(cachedRedirects.containsKey(origUrl)) {
												actualUrls.add(cachedRedirects.get(origUrl));
											} else {
												actualUrls.add(data.redirects().get(origUrl));
											}
										}
										
										for(String url : actualUrls) {
											if(url!=null) {
												for(String ht : alignment.hashtags()) {
													DBObject newItem = new BasicDBObject(3);
													newItem.put("hashtag", ht);
													newItem.put("url", url);
													newItem.put("tweet_id", alignment.tweetId());
													items.add(newItem);
												}
											}
										}
										if(items.size()>maxBulkSize*.9) {
											alignmentSink.store(items);
											items = new ArrayList<>(maxBulkSize);
										}
									}
									if(items.size()>0) {
										alignmentSink.store(items);
									}
								} catch (Throwable t) {
									t.printStackTrace();
								}
								
								rpt.inc(currentAlignments.size());
							}
						});
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
			TwitterDumpFileReader reader = new TwitterDumpFileReader(file).showProgress(false);
			ArrayList<DBObject> tweetStack = new ArrayList<>(chunkSize*10);
			
			
			for(DBObject tweet : reader) {
				if(tweet.containsField("urls") && tweet.get("urls") instanceof List && tweet.containsField("hashtags") && tweet.get("hashtags") instanceof List) {
					@SuppressWarnings("unchecked")
					final List<String> listOfUrls = (List<String>)tweet.get("urls");
					@SuppressWarnings("unchecked")
					final List<String> listOfHashtags = (List<String>)tweet.get("hashtags");
					
					// ignore spam tweets with particular hashtags
					boolean spam = false;
					for(String ht : listOfHashtags) {
						if("gameinsight".equalsIgnoreCase(ht) || "nowplaying".equalsIgnoreCase(ht) || "listenlive".equalsIgnoreCase(ht)) {
							spam = true;
							break;
						}
					}
					if(spam) {
						continue;
					}
					
					int alignmentCandidateCount;
					tweetStack.add(tweet);
					
					if(listOfUrls.size()>0 && listOfHashtags.size()>0) {
						synchronized (alignmentCandidates) {
							alignmentCandidates.add(new AlignmentCandidate(tweet.get("tweet_id"), listOfUrls, listOfHashtags));
							alignmentCandidateCount = alignmentCandidates.size();
						}
						while(alignmentCandidateCount>chunkSize*5) {
							// persist tweets
							if(tweetStack.size()>0) {
								tweetSink.store(tweetStack);
								tweetStack.clear();
							} else {
								try {
									synchronized (reader) {
										reader.wait(200);
									}
								} catch (InterruptedException e) {
									// ignore
								}
							}
							synchronized (alignmentCandidates) {
								alignmentCandidateCount = alignmentCandidates.size();
							}
						}
					}
				}
			}
		}
		finished = true;
		System.out.println(new Date().toString());
		
		try {
			synchronized (this) {
				// wait for 60s (there might be some pending requests)
				this.wait(60000);
			}
			guard.close();
			crawler.close();
		} catch (InterruptedException e) {
			// ignore
		}
		System.out.println();
	}
	
	static class AlignmentCandidate {
		private final Object tweetId;
		private final List<String> originalUrls;
		private final List<String> hashtags;
		public AlignmentCandidate(Object tweetId, List<String> originalUrls, List<String> hashtags) {
			this.tweetId = tweetId;
			this.originalUrls = originalUrls;
			this.hashtags = hashtags;
		}
		public Object tweetId() {
			return tweetId;
		}
		public List<String> originalUrls() {
			return originalUrls;
		}
		public List<String> hashtags() {
			return hashtags;
		}
	}
}
