package de.hpi.fgis.database.mongodb;

import java.util.Arrays;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * 
 * straight forward mockup for DBObject storage in MongoDB supporting default
 * bulk insert handling
 * 
 * @author tongr
 * 
 */
public class MongoDBObjectManager extends AbstractMongoManager {
	private final DBCollection collection;
	private DataMerger merger;
	
	/**
	 * get the {@link DataMerger} instance
	 * @return the 
	 */
	public DataMerger getMerger() {
		return merger;
	}
	/**
	 * set the {@link DataMerger} instance
	 * @param merger the {@link DataMerger} instance
	 * @return this instance
	 */
	public MongoDBObjectManager setMerger(DataMerger merger) {
		this.merger = merger;
		return this;
	}

	/**
	 * creates a new manager (with the default {@link MongoConnection})
	 * @param dbCollectionName the collection name
	 * @param mergeDataByID specifies either to take the {@link IDDataMerger}, or no merger (the {@link DataMerger} can be changed via {@link MongoDBObjectManager#setMerger(DataMerger)})
	 */
	public MongoDBObjectManager(String dbCollectionName, boolean mergeDataByID) {
		this(dbCollectionName, MongoConnection.getInstance(), mergeDataByID);
	}

	/**
	 * creates a new manager
	 * @param dbCollectionName the collection name
	 * @param connection the {@link MongoConnection} to be used
	 * @param mergeDataByID specifies either to take the {@link IDDataMerger}, or no merger (the {@link DataMerger} can be changed via {@link MongoDBObjectManager#setMerger(DataMerger)})
	 */
	public MongoDBObjectManager(String dbCollectionName, MongoConnection connection, boolean mergeDataByID) {
		super(connection);
		this.collection= super.getMongoConnection().getCollection(dbCollectionName);
		if(mergeDataByID) {
			this.merger = new IDDataMerger();
		} else {
			this.merger = null;
		}
	}
	
	/**
	 * stores the specified {@link DBObject} instance(s)
	 */
	public void store(DBObject... instances) {
		if(instances==null || instances.length==0) {
			return;
		}
		if(instances.length==1) {
			DBObject newDocument = merger==null?instances[0]:merger.mergeWithDBObject(instances[0]);
			
			// do not use bulk insert, to guarantee storage
			this.collection.save(newDocument);
		} else {
			this.store(Arrays.asList(instances));
		}
	}
	
	/**
	 * stores the specified {@link DBObject} instances
	 */
	public void store(Iterable<? extends DBObject> instances) {
		if(instances==null) {
			return;
		}
		for(DBObject instance : instances) {
			DBObject newDocument = merger==null?instance:merger.mergeWithDBObject(instance);
			
			// use bulk insert, to improve performance
			bulkInsert(this.collection, newDocument);
		}
		commit();
	}
	/**
	 * returns one (the first) {@link DBObject} instance with the specified attribute value
	 * @param attribute the attribute to query for
	 * @param value the attribute value to query for
	 * @return the (first) {@link DBObject} instance matching to the query
	 */
	public DBObject findOne(String attribute, Object value) {
		DBObject query = new BasicDBObject(attribute, value);

		return this.collection.findOne(query);
	}
	/**
	 * returns one (the first) {@link DBObject} instance with the specified attribute values
	 * @param attributes the attributes to query for
	 * @param values the attribute values to query for
	 * @return the (first) {@link DBObject} instance matching to the query
	 */
	public DBObject findOne(String[] attributes, Object... values) {
		if(attributes==null||values==null || attributes.length==0 || attributes.length!=values.length) {
			throw new IllegalArgumentException();
		}
		
		DBObject query = new BasicDBObject(attributes[0], values[0]);
		for(int i=1; i<attributes.length; i++) {
			query.put(attributes[i], values[i]);
		}
		
		return this.collection.findOne(query);
	}
	/**
	 * returns an iterator of all {@link DBObject} instances
	 * @return the iterator with all {@link DBObject} instances
	 */
	public Iterable<DBObject> find() {
		return find(null);
	}
	/**
	 * returns an iterator of {@link DBObject} instances with the specified attribute value
	 * @param attribute the attribute to query for
	 * @param value the attribute value to query for
	 * @return the iterator with all {@link DBObject} instances matching to the query
	 */
	public Iterable<DBObject> find(String attribute, Object value) {
		DBObject query = new BasicDBObject(attribute, value);

		return find(query);
	}
	/**
	 * returns an iterator of {@link DBObject} instances with the specified attribute values
	 * @param attributes the attributes to query for
	 * @param values the attribute values to query for
	 * @return the iterator with all {@link DBObject} instances matching to the query
	 */
	public Iterable<DBObject> find(String[] attributes, Object... values) {
		if(attributes==null||values==null || attributes.length==0 || attributes.length!=values.length) {
			throw new IllegalArgumentException();
		}
		
		DBObject query = new BasicDBObject(attributes[0], values[0]);
		for(int i=1; i<attributes.length; i++) {
			query.put(attributes[i], values[i]);
		}
		return find(query);
	}
	/**
	 * returns an iterator of {@link DBObject} instances with the specified attribute value
	 * @param the query instance
	 * @return the iterator with all {@link DBObject} instances matching to the query
	 */
	protected Iterable<DBObject> find(DBObject query) {
		return new MongoIterable<DBObject>(query, this.collection) {
			protected DBObject transform(DBObject mongoRepresentation) {
				return mongoRepresentation;
			}
			
		};
	}

	public void ensureIndices(boolean unique, String... attributeNames) {
		super.commit();
		
		// add useful indices
		for(String attribute : attributeNames) {
			this.collection.ensureIndex(new BasicDBObject(attribute, 1), attribute, unique);
		}
	}

	public void ensureCombinedIndex(boolean unique, String... attributeNames) {
		super.commit();
		DBObject attributeCombination = new BasicDBObject();
		StringBuilder name = new StringBuilder();
		for(String attribute : attributeNames) {
			attributeCombination.put(attribute, 1);
			name.append(attribute).append('_');
		}
		
		// add index
		this.collection.ensureIndex(attributeCombination, name.toString(), unique);
	}
	
	@Override
	public void clean() {
		super.clean();
		this.collection.drop();
	}
	@Override
	protected void drop() {
		super.clean();
		this.collection.drop();
	}


	/**
	 * merges a data object with an (eventually) existing object in the data base
	 * @author tongr
	 *
	 */
	public static interface DataMerger {
		public DBObject mergeWithDBObject(DBObject newValues);
	}
	
	/**
	 * compares and merges data objects based on their _id fields
	 * @author tongr
	 *
	 */
	public class IDDataMerger implements DataMerger {
		@Override
		public DBObject mergeWithDBObject(DBObject newValues) {
			if(!newValues.containsField("_id")) {
				return newValues;
			}
			DBObject dbObject = findOne("_id", newValues.get("_id"));
			if(dbObject==null) {
				return newValues;
			}

			dbObject.putAll(newValues);
			return dbObject;
		}
	}
	public DBCollection collection() {
		return collection;
	}
}
