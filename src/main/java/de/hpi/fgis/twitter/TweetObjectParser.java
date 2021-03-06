package de.hpi.fgis.twitter;

import java.text.SimpleDateFormat;
import java.util.Locale;

import com.mongodb.DBObject;

import de.hpi.fgis.json.AttributeMultiplier;
import de.hpi.fgis.json.ChainedTransformation;
import de.hpi.fgis.json.FormatTransformator;
import de.hpi.fgis.json.NullFilter;
import de.hpi.fgis.json.RetainFilter;

/**
 * this class applies an {@link RetainFilter} for a given Tweet-object and parses <b>date values</b><sup>1</sup>.
 * from the Twitter API
 * 
 * <b>tweet attributes:</b>
 * <ul>
 * <li>tweet_id (from id)</li>
 * <li>created_at<sup>1</sup></li>
 * <li>text</li>
 * <li>lang</li>
 * <li>retweeted</li>
 * <li>in_reply_to_status_id</li>
 * <li>in_reply_to_user_id</li>
 * </ul>
 * 
 * <b>author information</b>
 * <ul>
 * <li>user_id (from user/id)</li>
 * </ul>
 * 
 * <b>contained entities</b>
 * <ul>
 * <li>hashtags/&#42; (from entities/hashtags/&#42;/text)</li>
 * <li>urls/&#42; (from entities/urls/&#42;/expanded_url)</li>
 * <li>user_mentions/&#42; (from entities/user_mentions/&#42;/id)</li>
 * </ul>
 * 
 * <b>place information</b>
 * <ul>
 * <li>place/id</li>
 * <li>place/country</li>
 * <li>place/full_name</li>
 * <li>place/place_type</li>
 * </ul>
 * 
 * <b>retweet attributes:</b>
 * <ul>
 * <li>retweeted_status/created_at<sup>1</sup></li>
 * <li>retweeted_status/tweet_id (from retweeted_status/id)</li>
 * <li>retweeted_status/text</li>
 * <li>retweeted_status/lang</li>
 * <li>retweeted_status/retweet_count</li>
 * </ul>
 * 
 * <b>author information</b>
 * <ul>
 * <li>retweeted_status/user_id (from retweeted_status/user/id)</li>
 * </ul>
 * 
 * <b>contained entities</b>
 * <ul>
 * <li>retweeted_status/hashtags/&#42; (from retweeted_status/entities/hashtags/&#42;/text)</li>
 * <li>retweeted_status/urls/&#42; (from retweeted_status/entities/urls/&#42;/expanded_url)</li>
 * <li>retweeted_status/user_mentions/&#42; (from retweeted_status/entities/user_mentions/&#42;/id)</li>
 * </ul>
 * 
 * <b>place information</b>
 * <ul>
 * <li>retweeted_status/place/id</li>
 * <li>retweeted_status/place/country</li>
 * <li>retweeted_status/place/full_name</li>
 * <li>retweeted_status/place/place_type</li>
 * </ul>
 * 
 * @author tonigr
 * 
 */
public class TweetObjectParser extends ChainedTransformation {
	public TweetObjectParser() {
		super();
		
		// copy entity lists
		AttributeMultiplier multiplier = new AttributeMultiplier();
		multiplier.addTransformation("id", "tweet_id")
				.addTransformation("user/id", "user_id")
				.addTransformation("entities/hashtags/*/text", "hashtags")
				.addTransformation("entities/urls/*/expanded_url", "urls")
				.addTransformation("entities/user_mentions/*/id", "user_mentions")
				
				.addTransformation("retweeted_status/id", "retweeted_status/tweet_id")
				.addTransformation("retweeted_status/user/id", "retweeted_status/user_id")
				.addTransformation("retweeted_status/entities/hashtags/*/text", "retweeted_status/hashtags")
				.addTransformation("retweeted_status/entities/urls/*/expanded_url", "retweeted_status/urls")
				.addTransformation("retweeted_status/entities/user_mentions/*/id", "retweeted_status/user_mentions")
				//.addTransformation("id", "_id")
				;
		this.addTransformation(multiplier);

		// parse dates
		// example date: Sun Jan 22 12:10:59 +0000 2012
		SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy",Locale.ENGLISH);
		format.setLenient(true);
		FormatTransformator dateFormat = new FormatTransformator(format, "created_at", "retweeted_status/created_at");
		this.addTransformation(dateFormat);
		
		// remove other attributes
		RetainFilter attributeFilter = new RetainFilter(
				"tweet_id",
				"created_at",
				"text",
				"lang",
				"retweeted",
				"in_reply_to_status_id",
				"in_reply_to_user_id",
				"user_id",
				"hashtags/*",
				"urls/*",
				"user_mentions/*",
				"place/id",
				"place/country",
				"place/full_name",
				"place/place_type",
				"retweeted_status/tweet_id",
				"retweeted_status/created_at",
				"retweeted_status/text",
				"retweeted_status/lang",
				"retweeted_status/retweet_count",
				"retweeted_status/user_id",
				"retweeted_status/hashtags/*",
				"retweeted_status/urls/*",
				"retweeted_status/user_mentions/*",
				"retweeted_status/place/id",
				"retweeted_status/place/country",
				"retweeted_status/place/full_name",
				"retweeted_status/place/place_type");

		this.addTransformation(attributeFilter);
		this.addTransformation(new NullFilter());
	}
	
	/**
	 * transforms the JSON data representation of the Twitter API into a clean format
	 * @param orig the Twitter API JSON data representation
	 * @return the "clean" object with:
	 * <ul>
	 * <li>id</li>
	 * <li>created_at<sup>*</sup></li>
	 * <li>text</li>
	 * <li>lang</li>
	 * <li>retweeted</li>
	 * <li>in_reply_to_status_id</li>
	 * <li>in_reply_to_user_id</li>
	 * </ul>
	 * 
	 * <b>author information</b>
	 * <ul>
	 * <li>user/id</li>
	 * <li>user/screen_name</li>
	 * <li>user/lang</li>
	 * </ul>
	 * 
	 * <b>contained entities</b>
	 * <ul>
	 * <li>entities/hashtags</li>
	 * <li>entities/urls</li>
	 * <li>entities/user_mentions</li>
	 * </ul>
	 * 
	 * <b>place information</b>
	 * <ul>
	 * <li>place/id</li>
	 * <li>place/country</li>
	 * <li>place/full_name</li>
	 * <li>place/place_type</li>
	 * </ul>
	 * 
	 * <b>retweet attributes:</b>
	 * <ul>
	 * <li>retweeted_status/created_at<sup>*</sup></li>
	 * <li>retweeted_status/id</li>
	 * <li>retweeted_status/text</li>
	 * <li>retweeted_status/lang</li>
	 * <li>retweeted_status/retweet_count</li>
	 * </ul>
	 * 
	 * <b>author information</b>
	 * <ul>
	 * <li>retweeted_status/user/id</li>
	 * <li>retweeted_status/user/screen_name</li>
	 * </ul>
	 * 
	 * <b>contained entities</b>
	 * <ul>
	 * <li>retweeted_status/entities/hashtags</li>
	 * <li>retweeted_status/entities/urls</li>
	 * <li>retweeted_status/entities/user_mentions</li>
	 * </ul>
	 * 
	 * <b>place information</b>
	 * <ul>
	 * <li>retweeted_status/place/id</li>
	 * <li>retweeted_status/place/country</li>
	 * <li>retweeted_status/place/full_name</li>
	 * <li>retweeted_status/place/place_type</li>
	 * </ul>
	 */
	public DBObject transform(DBObject orig) {
		return super.transform(orig);
	}
	
	
}
