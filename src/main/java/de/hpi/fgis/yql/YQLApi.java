package de.hpi.fgis.yql;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import com.mongodb.DBObject;

/**
 * this abstract class provides different possibilities to access the <a
 * href="http://developer.yahoo.com/yql/">Yahoo! Query Language (YQL)</a>
 * web-interface.
 * 
 * @author tonigr
 * 
 */
public abstract class YQLApi {
	private final String baseURI;

	public YQLApi() {
		this("http://query.yahooapis.com/v1/public/yql?");
	}

	public YQLApi(String baseURI) {
		this.baseURI = baseURI;
	}

	// // TODO authentificated access --> for authenticated use simpleyql
	// public YQLApi(OAuth auth) {
	// this("http://query.yahooapis.com/v1/yql/yql?");
	// ...
	// }

	/**
	 * parse the returned result string provided by the YQL REST API
	 * 
	 * @param is
	 *            the data string to be parsed
	 * @return the actual data object
	 */
	protected abstract DBObject parse(InputStream is);

	/**
	 * gets the format string used/provided by the YQL endpoint
	 * 
	 * @return
	 */
	protected abstract String format();

	/**
	 * perform the specified query
	 * 
	 * @param query
	 *            the query to be executed (for instance
	 *            "select * from weather.forecast where woeid=638242" gets the
	 *            weather forecast for Berlin, Germany)
	 * @return a {@link DBObject} representation of the results (i.e. the
	 *         forcast data)
	 * @throws IOException
	 *             if some network errors occur
	 */
	public DBObject query(String query) throws IOException {
		return query(query, (Collection<Entry<String, String>>) null);
	}

	/**
	 * perform the specified query
	 * 
	 * @param query
	 *            the query to be executed (for instance
	 *            "select * from weather where location='Berlin, Germany'" gets
	 *            the weather forecast for Berlin, Germany given the following
	 *            table definition)
	 * @param tableName
	 *            name of the custom datatable used in the query (i.e.
	 *            "weather")
	 * @param tableDefURI
	 *            url of the table definition file (i.e.
	 *            "<a href='http://www.datatables.org/weather/weather.bylocation.xml'>http://www.datatables.org/weather/weather.bylocation.xml</a>"
	 *            )
	 * @return a {@link DBObject} representation of the results (i.e. the
	 *         forcast data)
	 * @throws IOException
	 *             if some network errors occur
	 */
	public DBObject query(String query, String tableName, String tableDefURI)
			throws IOException {
		return query(query,
				Arrays.asList((Entry<String, String>) new SimpleEntry<>(
						tableName, tableDefURI)));
	}

	/**
	 * perform the specified query
	 * 
	 * @param query
	 *            the query to be executed (for instance
	 *            "select * from weather where location='Berlin, Germany'" gets
	 *            the weather forecast for Berlin, Germany given the following
	 *            table definition)
	 * @param tables
	 *            a map containing all custom datatable definitions -- keys:
	 *            name of the table used in the query (i.e. "weather"), values:
	 *            url of the table definition file (i.e.
	 *            "<a href='http://www.datatables.org/weather/weather.bylocation.xml'>http://www.datatables.org/weather/weather.bylocation.xml</a>"
	 *            )
	 * @return a {@link DBObject} representation of the results (i.e. the
	 *         forcast data)
	 * @throws IOException
	 *             if some network errors occur
	 */
	public DBObject query(String query, Map<String, String> tables)
			throws IOException {
		return query(query, tables.entrySet());
	}

	/**
	 * perform the specified query and return additional meta information
	 * 
	 * @param query
	 *            the query to be executed (for instance
	 *            "select * from weather where location='Berlin, Germany'" gets
	 *            the weather forecast for Berlin, Germany given the following
	 *            table definition)
	 * @param tables
	 *            a map containing all custom datatable definitions -- keys:
	 *            name of the table used in the query (i.e. "weather"), values:
	 *            url of the table definition file (i.e.
	 *            "<a href='http://www.datatables.org/weather/weather.bylocation.xml'>http://www.datatables.org/weather/weather.bylocation.xml</a>"
	 *            )
	 * @return a {@link DBObject} representation of the results (i.e. the
	 *         forcast data)
	 * @throws IOException
	 *             if some network errors occur
	 */
	public DBObject queryMeta(String query, Map<String, String> tables)
			throws IOException {
		return queryMeta(query, tables.entrySet(), true);
	}

	protected String convertStreamToString(InputStream is, String charset) {
		Scanner s = new Scanner(is, charset);
		try {
			s.useDelimiter("\\A");
			return s.hasNext() ? s.next() : "";
		} finally {
			s.close();
		}
	}

	private DBObject query(String query,
			Collection<Entry<String, String>> tableDefs) throws IOException {
		return (DBObject) ((DBObject) queryMeta(query, tableDefs, false).get(
				"query")).get("results");
	}

	// TODO another possibility is the env parameter linking to a table def
	// file: env=http://datatables.org/alltables.env
	private DBObject queryMeta(String query,
			Collection<Entry<String, String>> tableDefs, boolean debug)
			throws IOException {
		Map<String, String> parameters = new HashMap<>(2);
		parameters.put("q", toTableDefString(tableDefs) + query);
		parameters.put("callback", "");
		parameters.put("format", format());
		if (debug) {
			parameters.put("diagnostics", "true");
			// parameters.put("debug", "true");
		}

		String requestUri = baseURI + toParameterString(parameters.entrySet());

		URL url = new URL(requestUri);

		InputStream in = url.openStream();

		try {
			return parse(in);
		} finally {
			in.close();
		}
	}

	private String toTableDefString(Collection<Entry<String, String>> tableDefs) {
		if (tableDefs == null || tableDefs.size() == 0) {
			return "";
		}
		StringBuilder result = new StringBuilder();
		for (Entry<String, String> entry : tableDefs) {
			result.append("USE \"").append(entry.getValue()).append("\" AS ")
					.append(entry.getKey()).append(';');
		}

		return result.toString();
	}

	private String toParameterString(Iterable<Entry<String, String>> parameters) {
		StringBuilder result = new StringBuilder();
		boolean first = true;
		for (Entry<String, String> entry : parameters) {
			if (first) {
				first = false;
			} else {
				result.append('&');
			}
			result.append(entry.getKey()).append('=')
					.append(escape(entry.getValue()));
		}

		return result.toString();
	}

	private String escape(String value) {
		try {
			return URLEncoder.encode(value, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new IllegalStateException("Unknown encoding UTF-8!", e);
		}
	}
}