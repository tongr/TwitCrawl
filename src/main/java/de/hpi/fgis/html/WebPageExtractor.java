package de.hpi.fgis.html;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.jsoup.Connection.Response;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import de.hpi.fgis.database.NoConnectionException;
import de.hpi.fgis.json.ITransformation;
/**
 * extends a given {@link DBObject} by the extracted HTML document content and meta information for a given URL (has to be part of the given {@link DBObject}) 
 * 
 * @author tongr
 * 
 */
public class WebPageExtractor implements ITransformation {
	private static final Logger logger =  Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	protected final static String TMP_META_ATTRIBUTE_NAME = "___META";
	protected final static String TMP_HTML_CONTENT_ATTRIBUTE_NAME = "___HTML";
	protected final static String TMP_TEXT_CONTENT_ATTRIBUTE_NAME = "___TEXT";
	protected static int TIMEOUT = 1500;
	private String urlAttribute = "url";
	private String metaAttribute = "meta";
	private String htmlAttribute = "html";
	private String textContentAttribute = "text";
	private boolean ignoreErrors = true;

	@Override
	public DBObject transform(DBObject orig) {
		if(!orig.containsField(getUrlAttribute())) {
			throw new IllegalArgumentException("Given instance does not contain URL attribute (" + urlAttribute + "): " + JSON.serialize(orig));
		}
		
		
		DBObject data = extractData(orig.get(getUrlAttribute()).toString());

		// add the extracted data to the orig instance
		orig.put(getMetaAttribute(), data.get(TMP_META_ATTRIBUTE_NAME));
		orig.put(getHtmlAttribute(), data.get(TMP_HTML_CONTENT_ATTRIBUTE_NAME));
		orig.put(getTextContentAttribute(), data.get(TMP_TEXT_CONTENT_ATTRIBUTE_NAME));

		return orig;
	}
	
	protected DBObject extractData(String url) {
		DBObject data = new BasicDBObject();
		
		try {
			// request the resource
			Response res = Jsoup.connect(url).userAgent("Mozilla/7.0 (X11; U; Linux i2058;) Gecko/Ubuntu/8.04 (hardy) Firefox/4.0.1 (alpha)").timeout(TIMEOUT).execute();
		
			// parse the document
			Document doc = res.parse();
		
			data.put(TMP_META_ATTRIBUTE_NAME, getMetaData(res, doc));
			data.put(TMP_HTML_CONTENT_ATTRIBUTE_NAME, doc.html());
			data.put(TMP_TEXT_CONTENT_ATTRIBUTE_NAME, doc.body().text());
		} catch (Exception e) {
			if(ignoreErrors()) {
				logger.log(Level.FINE, "Unable to load data from " + getUrlAttribute(), e);
			} else {
				logger.log(Level.SEVERE, "Unable to load data from " + getUrlAttribute(), e);
				throw new IllegalStateException("Unable to load data from " + getUrlAttribute(), e);
			}
		}

		return data;
	}
	
	protected DBObject getMetaData(Response res, Document doc) {
		DBObject metaData = new BasicDBObject();
		if(res!=null) {
			metaData.putAll(res.headers());
		}
		if(doc!=null) {
			for(Element meta : doc.select("meta")) {
				metaData.put(meta.attr("name"), meta.attr("content"));
			}
			
			metaData.put("title", doc.title());
		}
		return metaData;
	}
	
	
	public boolean ignoreErrors() {
		return ignoreErrors;
	}

	public WebPageExtractor setIgnoreErrors(boolean ignoreErrors) {
		this.ignoreErrors = ignoreErrors;
		return this;
	}

	public String getUrlAttribute() {
		return urlAttribute;
	}

	public WebPageExtractor setUrlAttribute(String urlAttribute) {
		this.urlAttribute = urlAttribute;
		return this;
	}

	public String getMetaAttribute() {
		return metaAttribute;
	}

	public WebPageExtractor setMetaAttribute(String metaAttribute) {
		this.metaAttribute = metaAttribute;
		return this;
	}

	public String getTextContentAttribute() {
		return textContentAttribute;
	}

	public WebPageExtractor setTextContentAttribute(String textContentAttribute) {
		this.textContentAttribute = textContentAttribute;
		return this;
	}
	

	public String getHtmlAttribute() {
		return htmlAttribute;
	}

	public WebPageExtractor setHtmlAttribute(String htmlAttribute) {
		this.htmlAttribute = htmlAttribute;
		return this;
	}
}
