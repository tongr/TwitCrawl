package de.hpi.fgis.json;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.mongodb.DBObject;

/**
 * represents a simple list filter for attributes to be "retained" after the application  
 * @author tongr
 *
 */
public class RetainFilter implements ITransformation {
	private final Map<String,Object> retainMap = new HashMap<String,Object>();
	
	/**
	 * creates a retain filter
	 * @param retainList the set of attributes to be retained (use "/" to split nested attributes)
	 */
	public RetainFilter(String... retainList) {
		this(Arrays.asList(retainList));
	}
	
	/**
	 * creates a retain filter
	 * @param retainList the set of attributes to be retained (use "/" to split nested attributes)
	 */
	@SuppressWarnings("unchecked")
	public RetainFilter(Collection<String> retainList) {
		Set<String> retainSet = new HashSet<String>(retainList);
		for(String attr : retainSet) {
			Map<String,Object> currentLayer = retainMap;
			String[] nestedAttr = attr.split("/");
			for(String currentAttr : nestedAttr) {
				if(currentLayer.containsKey(currentAttr)) {
					currentLayer = (Map<String,Object>) currentLayer.get(currentAttr);
				} else {
					Map<String,Object> nextLayer = new HashMap<String,Object>();
					currentLayer.put(currentAttr, nextLayer);
					currentLayer = nextLayer;
				}
			}
		}
	}
	
	
	/* (non-Javadoc)
	 * @see de.hpi.fgis.json.IFilter#filter(com.mongodb.DBObject)
	 */
	@Override
	public DBObject transform(DBObject orig) {
		filterRecursively(orig, retainMap);
		
		return orig;
	}
	
	@SuppressWarnings("unchecked")
	protected static void filterRecursively(DBObject orig, Map<String,Object> retainMap) {
		Set<String> actualAttributes = new HashSet<String>(orig.keySet());
		for(String currentAttr : actualAttributes) {
			// if asterix -> retain all attributes
			if(retainMap.containsKey("*")) {
				Object val = orig.get(currentAttr);
				
				if(val != null && val instanceof DBObject && ((Map<String,Object>)retainMap.get("*")).size()>0) {
					filterRecursively((DBObject) val, (Map<String,Object>) retainMap.get("*"));
				}
			}
			// not to be retained?
			else if(!retainMap.containsKey(currentAttr)) {
				//orig.removeField(currentAttr);
				orig.put(currentAttr, null);
			} else {
				Object val = orig.get(currentAttr);
				
				if(val != null && val instanceof DBObject) {
					// value is an "nested" json object -> investigate further
					filterRecursively((DBObject) val, (Map<String,Object>) retainMap.get(currentAttr));

//					// only empty json object left?
//					if(((DBObject)val).keySet().size()<=0) {
//						orig.removeField(currentAttr);
//					}
				}
			}
		}
	}
}
