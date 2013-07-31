package de.hpi.fgis.yql;

import de.hpi.fgis.concurrency.APIAccessRateLimitGuard;

/**
 * this singleton enables the execution of several public YQL API that underlies
 * a rate limits (i.e., 2,000 requests/hour per IP),
 * 
 * @see <a href='http://developer.yahoo.com/yql/guide/usage_info_limits.html'>
 *      http://developer.yahoo.com/yql/guide/usage_info_limits.html </a>
 * @author tongr
 * 
 */
public class YQLAccessRateLimitGuard extends APIAccessRateLimitGuard {
	private static YQLAccessRateLimitGuard INSTANCE = new YQLAccessRateLimitGuard();

	/**
	 * gets the {@link YQLAccessRateLimitGuard} singleton instance
	 * 
	 * @return the {@link YQLAccessRateLimitGuard} singleton instance
	 */
	public static YQLAccessRateLimitGuard getInstance() {
		return INSTANCE;
	}

	private YQLAccessRateLimitGuard() {
		// 2000 requests in 3 600 000ms (1h) --> 1800ms delay + 46ms as buffer
		// (thus max. requests ~1950)
		super(10, 1846);
	}
}
