package de.hpi.fgis.json;

import java.util.HashSet;
import java.util.Set;

import com.mongodb.DBObject;

/**
 * represents as simple list for attributes to be "retained" after the filter application  
 * @author tongr
 *
 */
public class NullFilter implements ITransformation {
	
	/**
	 * creates a filter
	 */
	public NullFilter() {
	}
	
	
	/* (non-Javadoc)
	 * @see de.hpi.fgis.json.IFilter#filter(com.mongodb.DBObject)
	 */
	@Override
	public DBObject transform(DBObject orig) {
		filterRecursively(orig);
		
		return orig;
	}
	
	protected static void filterRecursively(DBObject orig) {
		Set<String> actualAttributes = new HashSet<String>(orig.keySet());
		for(String currentAttr : actualAttributes) {
			Object val = orig.get(currentAttr);
			
			if(val == null) {
				orig.removeField(currentAttr);
			} else if(val instanceof DBObject) {
				// value is an "nested" json object -> investigate fuurther
				filterRecursively((DBObject) val);

				// only empty json object left?
				if(((DBObject)val).keySet().size()<=0) {
					orig.removeField(currentAttr);
				}
			}
		}
	}
}
