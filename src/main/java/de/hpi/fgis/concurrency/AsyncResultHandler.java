package de.hpi.fgis.concurrency;

import com.mongodb.DBObject;

import de.hpi.fgis.yql.YQLApi;

/**
 * this interface enables the asynchronous execution of {@link YQLApi} calls
 * 
 * @author tongr
 *
 * @param <T> the expected content type
 */
public interface AsyncResultHandler<T> {
	/**
	 * is called after completing the request
	 * 
	 * @param data
	 *            a {@link DBObject} representation of the results (i.e. the
	 *            forcast data)
	 */
	public abstract void onCompleted(T data);

	/**
	 * is called after some exceptions occurred
	 * 
	 * @param t
	 *            the {@link Throwable} that has been thrown
	 */
	public abstract void onThrowable(Throwable t);
}
