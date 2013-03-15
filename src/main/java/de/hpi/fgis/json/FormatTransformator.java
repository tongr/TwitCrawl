package de.hpi.fgis.json;

import java.text.DateFormat;
import java.text.Format;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import com.mongodb.DBObject;

/**
 * transforms all attributes to a date Type that removes the _id attribute  
 * @author tongr
 *
 */
public class FormatTransformator implements ITransformation {
	private final Collection<String> transformations;
	private final Format formatter;
	
	/**
	 * creates a format transformator
	 * @param retainList the set of attributes to be formatted (use "/" to split nested attributes)
	 */
	public FormatTransformator(Format formatter, String... transformList) {
		this(formatter, Arrays.asList(transformList));
	}
	
	/**
	 * creates a format transformator
	 * @param retainList the set of attributes to be formatted (use "/" to split nested attributes)
	 */
	public FormatTransformator(Format formatter, Collection<String> retainList) {
		this.transformations = new LinkedList<String>(retainList);
		this.formatter = formatter;
	}
	

	/**
	 * adds a new transformation path
	 * 
	 * @param path
	 *            the path to be added
	 * @return this instance
	 */
	public FormatTransformator addTransformation(
			String path) {
		if (path != null) {
			this.transformations.add(path);
		}
		return this;
	}
	
	/* (non-Javadoc)
	 * @see de.hpi.fgis.json.IFilter#filter(com.mongodb.DBObject)
	 */
	@Override
	public DBObject transform(DBObject orig) {
		for(String transformation : transformations) {
			String[] path = transformation.split("/");
			transformRecursively(orig, path, 0);
		}
		return orig;
	}
	
	private Object format(Object data) {
		if(data instanceof String) {
			try {
				return formatter.parseObject((String) data);
			} catch (ParseException e) {
				// ignore errors and return "empty" instance -> pars result = null
				return null;
			}
		// data stays date
		} else if(data instanceof DateFormat && data instanceof Date) {
			return data;
		// data stays number
		} else if(data instanceof NumberFormat && data instanceof Number) {
			return data;
		} else {
			return null;
		}
	}
	
	private void transformRecursively(DBObject data, String[] path, int currentPathLevel) {
		if(data==null || currentPathLevel>=path.length) {
			return;
		}
		
		if("*".equals(path[currentPathLevel])) {
			Set<String> keys = new HashSet<String>(data.keySet());
			if(currentPathLevel==path.length-1) {
				for(String key : keys) {
					data.put(key, this.format(data.get(key)));
				}
			} else {
				for(String key : keys) {
					Object next = data.get(key);		
					if(next instanceof DBObject) {
						transformRecursively((DBObject)next, path, currentPathLevel+1);
					} 
				}
			}
			return;
		}
		String key = path[currentPathLevel];
		Object val = data.get(key);
		if(currentPathLevel==path.length-1) {
			data.put(key, this.format(val));
		} else if(val instanceof DBObject) {
			transformRecursively((DBObject)val, path, currentPathLevel+1);
		} 
	}
	
}
