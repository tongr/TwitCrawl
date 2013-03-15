package de.hpi.fgis.json;

import com.mongodb.DBObject;

/**
 * this interface enables the transformation of attribute data of {@link DBObject} instances.
 * @author tongr
 *
 */
public interface ITransformation {
	/**
	 * transforms the given instance data
	 * @param orig the instance to be filtered
	 * @return the filtered {@link DBObject} instance (same reference as the parameter) 
	 */
	public abstract DBObject transform(DBObject orig);
}