package de.hpi.fgis;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import de.hpi.fgis.concurrency.APIAccessRateLimitGuard.RateLimitedTask;
import de.hpi.fgis.concurrency.AsyncResultHandler;
import de.hpi.fgis.database.mongodb.CachedMongoDBObjectManager;
import de.hpi.fgis.database.mongodb.MongoDBObjectManager;
import de.hpi.fgis.twitter.TwitterDumpFileReader;
import de.hpi.fgis.util.FileUtil;
import de.hpi.fgis.util.ProgressReport;
import de.hpi.fgis.yql.DeserializationException;
import de.hpi.fgis.yql.YQLAccessRateLimitGuard;
import de.hpi.fgis.yql.YQLCrawler;
import de.hpi.fgis.yql.YQLCrawler.CrawlingResults;

/**
 * Basic executor to parse a tweet file and crawl all contained urls via YQL
 * @author tongr
 */
public class YQLDumpFileCrawler implements Closeable {
	public static void main(String[] args) {
		String folder;
		if(args.length<=0) {
			//folder = "./";
			throw new IllegalArgumentException("Please specify an input folder!");
		} else {
			folder = args[0];
		}
		
		String[] files = new FileUtil().scan(folder, "tweets_w_links_n_htags_.*\\.stream", false).toArray(new String[0]);
		Arrays.sort(files);
		
		try (YQLDumpFileCrawler crawler = new YQLDumpFileCrawler()) {
			crawler.parse(files);
		}
	}
	protected static final Logger LOG = Logger.getLogger(YQLDumpFileCrawler.class.getName());
	private boolean finished = false;
	private final int chunkSize = 100;
	private final CachedMongoDBObjectManager redirectMan = new CachedMongoDBObjectManager(new MongoDBObjectManager("redirects", false), "from", 1000000, true);
	private final MongoDBObjectManager webpageSink = new MongoDBObjectManager("webpages", false);
	private final MongoDBObjectManager alignmentSink = new MongoDBObjectManager("alignments", false);
	private final MongoDBObjectManager allAlignmentSink = new MongoDBObjectManager("all_alignments", false);
	private final MongoDBObjectManager tweetSink = new MongoDBObjectManager("tweets", false);
	

	private final Set<String> spamHashTags = new HashSet<>(Arrays.asList("gameinsight", "nowplaying", "listenlive"));
	
	private void parse(String... files) {
		System.out.println(new Date().toString());
		
		final Queue<AlignmentCandidate> alignmentCandidates = new LinkedList<>();
		
		Closeable taskCloser = initJobs(alignmentCandidates);
		addAlignmentTasks(alignmentCandidates, files);
		
		System.out.println(new Date().toString());
		
		try {
			synchronized (this) {
				// wait for 10min (there might be some pending requests)
				this.wait(600000);
			}
			taskCloser.close();
		} catch (InterruptedException e) {
			// ignore
		} catch (Exception e) {
			LOG.log(Level.WARNING, "Unable to close pending modules", e);
		}
	}
	
	private Closeable initJobs(final Queue<AlignmentCandidate> alignmentCandidates) {
		final YQLAccessRateLimitGuard guard = YQLAccessRateLimitGuard.getInstance();
		final YQLCrawler crawler = new YQLCrawler();
		final Queue<AlignmentCandidate> retryAlignmentCandidates = new LinkedList<>();
		final ProgressReport rpt = new ProgressReport("Crawling urls from tweets ...").setUnit("tweets").setReport(2500);
		RateLimitedTask task = new RateLimitedTask() {
			@Override
			public void run() {
				try {
					final ArrayList<String> toBeCrawled = new ArrayList<>(chunkSize*2);
					final ArrayList<AlignmentCandidate> currentAlignments = new ArrayList<>(chunkSize);
					final HashMap<String, String> cachedRedirects = new HashMap<>();
					
					boolean retry = false;
					synchronized (retryAlignmentCandidates) {
						if(retryAlignmentCandidates.size()>0) {
							
							while(toBeCrawled.size()<chunkSize && retryAlignmentCandidates.size()>0) {
								AlignmentCandidate candidate = retryAlignmentCandidates.poll();
								currentAlignments.add(candidate);
								
								for(String url : candidate.originalUrls()) {
									DBObject redirect = redirectMan.findOne(url);
									
									if(redirect==null) {
										toBeCrawled.add(url);
									} else {
										cachedRedirects.put(url, (String) redirect.get("to")); 
									}
								}
							}
							
							retry = true;
						}
					}
					if(!retry) {
						synchronized (alignmentCandidates) {
							while(toBeCrawled.size()<chunkSize && alignmentCandidates.size()>0) {
								AlignmentCandidate candidate = alignmentCandidates.poll();
								currentAlignments.add(candidate);
								
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
					}
					
					if(toBeCrawled.size()>0) {
						final boolean isRetry = retry;
						crawler.crawlAsync(toBeCrawled, new AsyncResultHandler<CrawlingResults>() {
							
							@Override
							public void onThrowable(Throwable t) {
								if(!(t instanceof IOException || t instanceof DeserializationException)) {
									LOG.log(Level.WARNING, "Unexpected error occured!", t);
								} else if (isRetry) {
									LOG.log(Level.WARNING, "Some data extraction problems occured repeatedly!", t);
								} else {
									LOG.log(Level.INFO, "Some data extraction problems occured, retrying in several seconds ... ", t);
									synchronized (retryAlignmentCandidates) {
										retryAlignmentCandidates.addAll(currentAlignments);
									}
								}
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
					crawler.close();
					guard.close();
					return false;
				}
				return true;
			}
		};
		guard.add(task);
		
		return new Closeable() {
			
			@Override
			public void close() throws IOException {
				guard.close();
				crawler.close();
			}
		};
	}

	private void addAlignmentTasks(final Queue<AlignmentCandidate> alignmentCandidates, String... files) {
		ArrayList<DBObject> tweetStack = new ArrayList<>(chunkSize*10);
		ArrayList<DBObject> allAlignmentItems = new ArrayList<>(chunkSize*10);
		for(String file : files) {
			System.out.print("parsing tweets of: ");
			System.out.println(file);
			TwitterDumpFileReader reader = new TwitterDumpFileReader(file, false).showProgress(false);
			
			for(DBObject tweet : reader) {
				if(tweet.containsField("urls") && tweet.get("urls") instanceof List && tweet.containsField("hashtags") && tweet.get("hashtags") instanceof List) {
					@SuppressWarnings("unchecked")
					final List<String> listOfUrls = (List<String>)tweet.get("urls");
					@SuppressWarnings("unchecked")
					final List<String> listOfHashtags = (List<String>)tweet.get("hashtags");
					
					// try to figure out whether this is a spam tweet (contains particular hashtags)
					boolean spam = false;
					for(String ht : listOfHashtags) {
						if( ht!=null && spamHashTags.contains(ht.toLowerCase()) ) {
							spam = true;
							break;
						}
					}
					
					// store tweet alignments (unresolved)
					for(String url : new HashSet<>(listOfUrls)) {
						if(url!=null) {
							for(String ht : new HashSet<>(listOfHashtags)) {
								if(ht != null) {
									DBObject newItem = new BasicDBObject(3);
									newItem.put("hashtag", ht);
									newItem.put("url", url);
									newItem.put("tweet_id", tweet.get("tweet_id"));
									newItem.put("spam", spam);
									allAlignmentItems.add(newItem);
								}
							}
						}
					}
					if(allAlignmentItems.size()>chunkSize*5) {
						allAlignmentSink.store(allAlignmentItems);
						allAlignmentItems.clear();
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
		if(tweetStack.size()>0) {
			tweetSink.store(tweetStack);
			tweetStack.clear();
		}
		if(allAlignmentItems.size()>0) {
			allAlignmentSink.store(allAlignmentItems);
			allAlignmentItems.clear();
		}
		finished = true;
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

	@Override
	public void close() {
		redirectMan.close();
		webpageSink.close();
		alignmentSink.close();
		allAlignmentSink.close();
		tweetSink.close();
	}
}
