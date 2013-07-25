package de.hpi.fgis.html;

import java.util.logging.Level;
import java.util.logging.Logger;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ExtractorBase;
import de.l3s.boilerpipe.extractors.KeepEverythingExtractor;

/**
 * this class serves as a mapper to the different boilerpipe methods
 * @author tongr
 *
 */
public enum ContentExtractor {
	ArticleExtractor(de.l3s.boilerpipe.extractors.ArticleExtractor.INSTANCE),
	DefaultExtractor(de.l3s.boilerpipe.extractors.DefaultExtractor.INSTANCE),
	CompleteTextExtractor(KeepEverythingExtractor.INSTANCE),
	CanolaExtractor(de.l3s.boilerpipe.extractors.CanolaExtractor.INSTANCE);
	
	private static final Logger logger =  Logger.getLogger(ContentExtractor.class.getName());
	private final ExtractorBase extractor;
	private ContentExtractor(ExtractorBase extractor) {
		this.extractor = extractor;
	}
	/**
	 * extract the text of a given html snippet using the underlying boilerpipe implementation
	 * @param html the html content
	 * @return
	 */
	public String extractText(String html) {
		if(html==null || html.isEmpty()) {
			return "";
		}
		try {
			return this.extractor.getText(html);
		} catch (BoilerpipeProcessingException e) {
			if( logger.isLoggable(Level.FINE)) {
				logger.log(Level.FINE, "Unable to parse HTML content", e);
			}
			return "";
		}
	}
}
