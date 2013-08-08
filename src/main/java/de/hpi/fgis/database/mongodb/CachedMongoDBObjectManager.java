package de.hpi.fgis.database.mongodb;

import java.util.Arrays;

import org.apache.lucene.facet.collections.LRUHashMap;

import com.mongodb.DBObject;

/**
 * straight forward mockup for DBObject storage in MongoDB supporting cached
 * access to the latest instances (LRU cache)
 * 
 * @author tongr
 * 
 */
public class CachedMongoDBObjectManager {
	private final MongoDBObjectManager persistence;
	private final String keyAttribute;
	private final LRUHashMap<Object, DBObject> cache;
	private final boolean checkPersisted;

	/**
	 * creates a new instance of the cached MongoDB storage interface
	 * 
	 * @param persistence
	 *            the actual MongoDB storage instance used for persistence
	 * @param keyAttribute
	 *            the attribute name within the persisted objects to be checked
	 *            for "cache hits"
	 * @param cacheSize
	 *            the size of the cache
	 * @param checkCacheOnly
	 *            this value enables a performance optimization for special
	 *            scenarios. If this value is set to <code>true</code>, the
	 *            stored instances are not checked for containment in the
	 *            persistence source in case of a
	 *            {@link CachedMongoDBObjectManager#findOne(Object)} call,
	 *            otherwise (<code>false</code>) the persistence source will be
	 *            checked thus the method call will take more much more time.
	 */
	public CachedMongoDBObjectManager(MongoDBObjectManager persistence,
			String keyAttribute, int cacheSize, boolean checkCacheOnly) {
		this.persistence = persistence;
		this.keyAttribute = keyAttribute;
		this.cache = new LRUHashMap<>(cacheSize);
		this.checkPersisted = !checkCacheOnly;

		if (checkPersisted) {
			// add an index for more performance of {@link
			// CachedMongoDBObjectManager#findOne(Object)} calls
			persistence.ensureIndices(false, keyAttribute);
		}
	}

	/**
	 * stores & caches the specified {@link DBObject} instance(s)
	 */
	public void store(DBObject... instances) {
		if (instances == null || instances.length == 0) {
			return;
		}
		if (instances.length == 1) {
			// cache the value
			cache(instances[0]);

			// evt. do not use bulk insert, to guarantee storage
			persistence.store(instances[0]);
		}
		this.store(Arrays.asList(instances));
	}

	/**
	 * stores & caches the specified {@link DBObject} instances
	 */
	public void store(Iterable<? extends DBObject> instances) {
		// cache the values
		cache(instances);

		persistence.store(instances);
	}

	/**
	 * returns one (i.e., the latest cached / the first) {@link DBObject}
	 * instance with the specified value of the key attribute
	 * 
	 * @param value
	 *            the value of the key attribute to query for
	 * @return the (latest cached / first) {@link DBObject} instance matching to
	 *         the query
	 */
	public DBObject findOne(Object value) {
		// check cache
		DBObject foundInstance = checkCache(value);

		// cache miss --> ask persistence?
		if (checkPersisted && foundInstance == null) {
			foundInstance = persistence.findOne(keyAttribute, value);

			if (foundInstance != null) {
				cache(foundInstance);
			}
		}

		return foundInstance;
	}

	private DBObject checkCache(Object value) {
		synchronized (cache) {
			// check cache map and return found value
			return cache.get(value);
		}
	}

	private void cache(DBObject obj) {
		final Object key = obj.get(keyAttribute);
		if (key != null) {
			synchronized (cache) {
				cache.put(key, obj);
			}
		}
	}

	protected void cache(Iterable<? extends DBObject> objs) {
		if (objs == null) {
			return;
		}

		synchronized (cache) {
			// update cache map with the object
			for (DBObject obj : objs) {
				final Object key = obj.get(keyAttribute);
				if (key != null) {
					cache.put(key, obj);
				}
			}
		}
	}
}
