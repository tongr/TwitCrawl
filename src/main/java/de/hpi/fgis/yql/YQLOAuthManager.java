package de.hpi.fgis.yql;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.FluentStringsMap;
import com.ning.http.client.ListenableFuture;
import com.ning.http.client.Request;
import com.ning.http.client.Response;
import com.ning.http.client.oauth.ConsumerKey;
import com.ning.http.client.oauth.OAuthSignatureCalculator;
import com.ning.http.client.oauth.RequestToken;
import com.ning.http.util.Base64;

public class YQLOAuthManager extends AsyncHttpClient {
	private static final Logger logger = Logger.getLogger(YQLOAuthManager.class
			.getName());

	private final ConsumerKey oAuthConsumerKey;
	private final String oAuthUri = "https://api.login.yahoo.com/oauth/v2/get_request_token";

	private synchronized static ConsumerKey loadConsumerKey() {
		// load cfg file
		Properties prop = new Properties();

		try {
			// load a properties file
			prop.load(YQLOAuthManager.class
					.getResourceAsStream("/yql.private.conf"));

		} catch (IOException ex) {
			logger.log(Level.SEVERE, "Unable to read YQL configuration file!",
					ex);
		}

		if (prop.containsKey("oauth_consumer_key")
				&& prop.containsKey("oauth_signature")) {
			return new ConsumerKey(prop.getProperty("oauth_consumer_key"),
					prop.getProperty("oauth_signature"));
		}
		return null;
	}

	protected YQLOAuthManager() {
		this.oAuthConsumerKey = loadConsumerKey();
		
		// print log in case of missing credentials
		if(!useAuthentication()) {
			logger.log(Level.WARNING, "Unable to use authentication for yql, proper consumer key configuration is missing! Using public fallback ...");
		}
	}

	@Override
	public ListenableFuture<Response> executeRequest(Request request)
			throws IOException {
		// TODO Auto-generated method stub
		return super.executeRequest(request);
	}

	/**
	 * tells weather the authentication for yql is properly configured or not
	 * 
	 * @see <a
	 *      href="http://developer.yahoo.com/yql/guide/usage_info_limits.html"
	 *      >http://developer.yahoo.com/yql/guide/usage_info_limits.html</a>
	 * @return <code>true</code>, if the authentication can be used to process
	 *         YQL queries, <code>false</code> if the public endpoint has to be
	 *         addressed
	 */
	public boolean useAuthentication() {
		return oAuthConsumerKey != null;
	}

	/*
	 * phase 2 --> fetch request token
	 */
	private HashMap<String, String> fetchOAuthToken() throws IOException {
		Random random = new Random();
		byte[] nonceBuffer = new byte[16];
		random.nextBytes(nonceBuffer);

		FluentStringsMap parameters = new FluentStringsMap();
		parameters.add("oauth_consumer_key", oAuthConsumerKey.getKey());
		parameters.add("oauth_nonce", Base64.encode(nonceBuffer));
		// HMAC-SHA1 or plaintext
		parameters.add("oauth_signature_method", "plaintext");
		// +"&" in case of plaintext
		parameters.add("oauth_signature", oAuthConsumerKey.getSecret() + "&");
		parameters.add("oauth_timestamp",
				Long.toString(System.currentTimeMillis() / 1000L));
		parameters.add("oauth_version", "1.0");
		// optional xoauth_lang_pref
		parameters.add("oauth_callback", "oob");

		try {
			ListenableFuture<Response> responseFuture = preparePost(oAuthUri)
					.setParameters(parameters).execute();

			Response r = responseFuture.get();

			String data = r.getResponseBody("UTF-8");

			HashMap<String, String> tokenData = new HashMap<>();
			for (String pair : data.split("&")) {
				String[] pairData = pair.split("=");
				if (pairData != null && pairData.length == 2) {
					tokenData.put(pairData[0], pairData[1]);
				}
			}

			return tokenData;
		} catch (InterruptedException | ExecutionException
				| IllegalArgumentException | IOException e) {
			throw new IOException("Unable to fetch YQL OAuth token!", e);
		}
	}
	


	private void authenticate() throws IOException {
		// FIXME see http://nullinfo.wordpress.com/oauth-yahoo/
		if(!useAuthentication()) {
			this.setSignatureCalculator(null);
			return;
		}
		synchronized (this) {
			long timeStamp = System.currentTimeMillis();
			
			HashMap<String, String> tokenData = fetchOAuthToken();

			// expires in X seconds, save expiring date X - 5min
			final long oAuthExpiresAt = System.currentTimeMillis()
					+ Long.parseLong(tokenData.get("oauth_expires_in"))
					* 1000L - 300000;

			
			// start phase 3 instead
			this.setSignatureCalculator(new OAuthSignatureCalculator(
					oAuthConsumerKey,
					new RequestToken(tokenData.get("oauth_token"),
							tokenData.get("oauth_token_secret"))));
		
			
			// TODO start Timer that will be executed at oAuthExpiresAt and gets a new oatuh token
			
			
		}
		// final AsyncHttpClient asyncClient = new AsyncHttpClient();
		//
		// if(this.useAuthenticatedEndpoint()) {
		// RequestToken token = fetchOAuthToken(asyncClient);
		//
		// asyncHttpClient.executeRequest(request, handler)
		// // asyncHttpClient.setSignatureCalculator(new
		// OAuthSignatureCalculator(
		// // new ConsumerKey("", ""),
		// // ,new RequestToken("", "")));
		// }
		//
	}

//	public static void main(String[] args) throws IOException {
//		YQLApi api = new YQLApiJSON(
//				"dj0yJmk9TGJCN2c0N3B1ZWE4JmQ9WVdrOU5VZFpNalp2TkdNbWNHbzlNakUzTmpBMk1qWXkmcz1jb25zdW1lcnNlY3JldCZ4PWQ5",
//				"b88432a0dcdbeab5dd7155118c367275fabf2a9a");
//		// TODO run request
//		api.query("show tables");
//
//		// System.out.println(api.fetchOAuthToken(asyncHttpClient));
//		// System.out.println(api.fetchOAuthToken(asyncHttpClient));
//		// System.out.println(api.fetchOAuthToken(asyncHttpClient));
//		// System.out.println(api.fetchOAuthToken(asyncHttpClient));
//		// System.out.println(api.fetchOAuthToken(asyncHttpClient));
//		// System.out.println(api.fetchOAuthToken(asyncHttpClient));
//
//		// FluentStringsMap parameters = new FluentStringsMap();
//		// parameters.add("oauth_consumer_key", "")
//		//
//		//
//		// Request
//		// asyncHttpClient.executeRequest(request, handler)
//		// asyncHttpClient.setSignatureCalculator(new OAuthSignatureCalculator(
//		// new ConsumerKey("", ""),
//		// ,new RequestToken("", "")));
//		// RequestParams requestParams = new RequestParams();
//		api.close();
//	}

	@Override
	public void close() {
		super.close();

		// TODO close timer threads etc.
	}
}
