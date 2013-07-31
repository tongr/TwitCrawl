package de.hpi.fgis.yql;

import java.io.InputStream;
import java.util.Collection;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * this class provides the possibility to access the <a
 * href="http://developer.yahoo.com/yql/">Yahoo! Query Language (YQL)</a>
 * web-interface using the JSON serialization.
 * 
 * @author tonigr
 * 
 */
public class YQLApiXML extends YQLApi {
	/**
	 * create a new YQL API access instance that uses XML serialization and the public YQL endpoint
	 */
	public YQLApiXML() {
		super();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.hpi.fgis.yql.YQLApi#format()
	 */
	@Override
	protected String format() {
		return "xml";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.hpi.fgis.yql.YQLApi#parse(java.lang.String)
	 */
	@Override
	protected DBObject parse(InputStream xmlIn) {
		// inspired by Costis Aivalis and Nathan Hughes:
		// http://stackoverflow.com/a/7373596/2047219

		Document dom;
		// Make an instance of the DocumentBuilderFactory
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

		try {
			// use the factory to take an instance of the document builder
			DocumentBuilder db = dbf.newDocumentBuilder();

			// parse using the builder to get the DOM mapping of the XML file
			dom = db.parse(xmlIn);

			Element doc = dom.getDocumentElement();

			DBObject data = new BasicDBObject();

			fill(data, doc);

			return data;

		} catch (Exception e) {
			throw new IllegalStateException("Unable to parse XML!", e);
		}
	}

	@SuppressWarnings("unchecked")
	private void put(DBObject sink, String field, Object value) {
		if ("#text".equals(field)) {
			field = "content";
		}
		if (sink.containsField(field)) {
			Object prevValue = sink.get(field);
			if (prevValue instanceof Collection) {
				((Collection<Object>) prevValue).add(value);
			} else {
				BasicDBList list = new BasicDBList();
				list.add(prevValue);
				list.add(value);
				sink.put(field, list);
			}
		} else {
			sink.put(field, value);
		}
	}

	private void fill(DBObject sink, Node n) {
		if (n.getNodeValue() != null && !n.getNodeValue().trim().isEmpty()) {
			put(sink, n.getNodeName(), n.getNodeValue());
		} else {
			BasicDBObject content = new BasicDBObject();

			if (n.hasAttributes()) {
				fill(content, n.getAttributes());
			}

			if (n.hasChildNodes()) {
				fill(content, n.getChildNodes());
			}
			if (content.size() > 0) {
				if (content.size() == 1 && content.containsField("content")) {
					put(sink, n.getNodeName(), content.get("content"));
				} else {
					put(sink, n.getNodeName(), content);
				}
			}
		}
	}

	private void fill(DBObject sink, NamedNodeMap m) {
		for (int i = 0; i < m.getLength(); i++) {
			fill(sink, m.item(i));
		}
	}

	private void fill(DBObject sink, NodeList l) {
		for (int i = 0; i < l.getLength(); i++) {
			fill(sink, l.item(i));
		}
	}
}
