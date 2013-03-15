package de.hpi.fgis.twitter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;

import com.mongodb.DBObject;

import de.hpi.fgis.json.DBObjectStreamIterator;
import de.hpi.fgis.util.ProgressReport;

/**
 * enables the processing of stream dump files containing newline delimited json objects
 * @author tongr
 *
 */
public class TwitterDumpFileReader implements Iterable<DBObject> {
	final File inputFile;
	boolean showProgress;
	/**
	 * initializes a dump file reader
	 * @param filename the file name
	 */
	public TwitterDumpFileReader(String filename) {
		this(null, filename);
	}
	/**
	 * initializes a dump file reader
	 * @param path the file path
	 * @param filename the file name
	 */
	public TwitterDumpFileReader(String path, String filename) {
		inputFile = new File(path, filename);
		this.showProgress = true;
		
		if(!inputFile.exists()) {
			throw new IllegalArgumentException("Unable to open \"" + inputFile + "\"! File does not exist.");
		}
	}
	
	
	public boolean showProgress() {
		return showProgress;
	}
	public TwitterDumpFileReader setShowProgress(boolean showProgress) {
		this.showProgress = showProgress;
		return this;
	}
	
	@Override
	public Iterator<DBObject> iterator() {
		try {
			return new DBObjectStreamIterator(new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), "UTF8"))) {
				private ProgressReport rpt = showProgress?new ProgressReport("Parsing \"" + inputFile.getName() + "\" ...").setUnit("tweets").setReport(25000):null;
				private final TweetObjectParser parser = new TweetObjectParser();
				
				@Override
				public DBObject next() {
					if( rpt!=null ) {
						rpt.inc();
					}
					// clean data instance
					return parser.transform(super.next());
				}
			};
		} catch (FileNotFoundException e) {
			throw new IllegalStateException("Unable to open file \"" + inputFile + "\"!", e);
		} catch (UnsupportedEncodingException e) {
			throw new IllegalStateException("Woops! This should not happen!", e);
		}

	}

	
}
