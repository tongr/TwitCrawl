package de.hpi.fgis.json;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * copies the attribute values within a {@link DBObject}<br/>
 * <b>Attention!</b> this class does not handle problems related to concurrent chained modifications source/target attributes<br/>
 * For instance:<br/>
 * <code>
 * {@link AttributeMultiplier} mod = new {@link AttributeMultiplier}().addTransformation("a", "b").addTransformation("b", "a");
 * {@link System}.out.println(mod.transform({@link JSON}.parse("{a : '_a', b : '_b'}")));
 * </code><br/>
 * leads to<br/>
 * <code>{a : '_b', b : '_a'}</code>
 * 
 * @author tongr
 * 
 */
public class AttributeMultiplier implements ITransformation {
	private Map<String, Set<String>> transformations = new HashMap<String, Set<String>>();
	
	@Override
	public DBObject transform(DBObject orig) {
		DBObject copy = cloneRecursively(orig);
		HashSet<String> targetAttributes = new HashSet<String>(transformations.keySet());

		for(String currentTarget : targetAttributes) {
			for(String currentSource : transformations.get(currentTarget)) {
				String[] path = currentSource.split("/");
				set(currentTarget, get(copy, path, 0), orig);
			}
		}
		return orig;
	}
	private DBObject cloneRecursively(DBObject data) {
		if(data==null) {
			return null;
		} 
		DBObject copy = new BasicDBObject();
		for(String key : data.keySet()) {
			Object val = null;
			if(data.get(key)!=null) {
				if(data.get(key) instanceof DBObject) {
					val = cloneRecursively((DBObject) data.get(key));
//				} else if(data.get(key) instanceof List) {
//					val = new ArrayList<Object>(((List<?>) data.get(key)));
				} else {
					val = data.get(key);
				}
			}
			copy.put(key, val);
		}
			
		return copy;
	}
	
//	private Object get(String attribute, DBObject data) {
//		String[] path = attribute.split("/");
//		Object tmp = data;
//		for(String pathElement : path) {
//			if(tmp instanceof DBObject) {
//				tmp = ((DBObject)tmp).get(pathElement);
//			} else if(tmp instanceof List) {
//				tmp = ArrayList<Object>();
//				for(Object val : (List<?>)tmp) {
//					
//				}
//				return null;
//			} else {
//				return null;
//			}
//			
//		}
//		return tmp;
//	}
	private Object get(DBObject data, String[] path, int currentPathLevel) {
		if(data==null || currentPathLevel>=path.length) {
			return null;
		}
		
		if("*".equals(path[currentPathLevel])) {
			BasicDBList combined = new BasicDBList();
			
			if(currentPathLevel==path.length-1) {
				for(String k : data.keySet()) {
					combined.add(data.get(k));
				}
			} else {
				for(String k : data.keySet()) {
					Object next = data.get(k);		
					if(next instanceof DBObject) {
						combined.add(get((DBObject)next, path, currentPathLevel+1));
					} 
				}
			}
			
			combined.removeAll(Arrays.asList((Object)null));
			
			return combined;
		}
		Object val = data.get(path[currentPathLevel]);
		if(currentPathLevel==path.length-1) {
			return val;
		}
		if(val instanceof DBObject) {
			return get((DBObject)val, path, currentPathLevel+1);
		} 
		if(data instanceof List) {
			ArrayList<Object> combined = new ArrayList<Object>();
		
			for(Object o : (List<?>) val) {
				if(o instanceof DBObject) {
					combined.add(get((DBObject)o, path, currentPathLevel+1));
				}
			}
			
			combined.removeAll(Arrays.asList((Object)null));
			
			return combined;
		}
		
		return null;
		
	}
	
	private void set(String attribute, Object value, DBObject data) {
		if(value == null) {
			return;
		}
		String[] path = attribute.split("/");
		
		DBObject tmp = data;
		for(int i=0;i<path.length;i++) {
			String pathElement = path[i];
			if(i==path.length-1) {
				tmp.put(pathElement, value);
			} else if(tmp.containsField(pathElement) && tmp.get(pathElement) instanceof DBObject) {
				tmp = (DBObject) tmp.get(pathElement);
			} else {
				DBObject newInstance = new BasicDBObject();
				
				tmp.put(pathElement, newInstance);
				
				tmp = newInstance;
			}
			
		}
	}
	
	public AttributeMultiplier addTransformation(String oldAttribute, String newAttribute) {
		if(!transformations.containsKey(newAttribute)) {
			transformations.put(newAttribute, new HashSet<String>());
		}
		transformations.get(newAttribute).add(oldAttribute);
		
		return this;
	}
}
