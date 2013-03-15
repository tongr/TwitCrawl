package de.hpi.fgis.json;

import java.util.HashSet;
import java.util.Set;

import com.mongodb.DBObject;

/**
 * filter that removes the _id attribute  
 * @author tongr
 *
 */
public class IDFilter implements ITransformation {

	@Override
	public DBObject transform(DBObject orig) {
		//orig.removeField("_id");
		if(orig.containsField("_id")) {
			orig.put("_id", null);
		}
		Set<String> keys = new HashSet<String>(orig.keySet());
		for(String key : keys) {
			Object val = orig.get(key);
			if(val!=null && val instanceof DBObject) {
				transform((DBObject)val);

//				// only empty json object left?
//				if(((DBObject)val).keySet().size()<=0) {
//					orig.removeField(key);
//				}
			}
		}
		return orig;
	}

}
