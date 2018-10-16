package br.com.anteros.nosql.persistence.mongodb.mapping;

import static java.lang.String.format;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.types.CodeWScope;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;

import br.com.anteros.core.log.LogLevel;
import br.com.anteros.core.log.Logger;
import br.com.anteros.core.log.LoggerProvider;
import br.com.anteros.core.utils.ReflectionUtils;
import br.com.anteros.nosql.persistence.converters.Key;
import br.com.anteros.nosql.persistence.converters.NoSQLConverters;
import br.com.anteros.nosql.persistence.converters.NoSQLCustomConverters;
import br.com.anteros.nosql.persistence.converters.NoSQLMappingException;
import br.com.anteros.nosql.persistence.converters.NoSQLTypeConverter;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionEntity;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionEntityManager;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;
import br.com.anteros.nosql.persistence.metadata.annotations.AfterLoad;
import br.com.anteros.nosql.persistence.metadata.annotations.BeforeLoad;
import br.com.anteros.nosql.persistence.metadata.annotations.BeforeSave;
import br.com.anteros.nosql.persistence.mongodb.converters.NoSQLSerializer;
import br.com.anteros.nosql.persistence.mongodb.session.MongoSession;
import br.com.anteros.nosql.persistence.proxy.ProxiedEntityReference;
import br.com.anteros.nosql.persistence.proxy.ProxyHelper;
import br.com.anteros.nosql.persistence.session.NoSQLSession;
import br.com.anteros.nosql.persistence.session.cache.NoSQLEntityCache;
import br.com.anteros.nosql.persistence.session.mapping.AbstractNoSQLObjectMapper;
import br.com.anteros.nosql.persistence.session.mapping.NoSQLMapperOptions;

public class MongoObjectMapper extends AbstractNoSQLObjectMapper {

	public static final String ID_KEY = "_id";

	public static final String IGNORED_FIELDNAME = ".";

	public static final String CLASS_NAME_FIELDNAME = "className";

	private static final Logger LOG = LoggerProvider.getInstance().getLogger(MongoObjectMapper.class);

	private Map<Class<?>, Object> instanceCache = new ConcurrentHashMap<>();
	private NoSQLConverters converters;


	public MongoObjectMapper(NoSQLDescriptionEntityManager descriptionEntityManager, NoSQLMapperOptions opts) {
		super(descriptionEntityManager, opts);
	}

	public MongoObjectMapper(NoSQLDescriptionEntityManager descriptionEntityManager) {
		super(descriptionEntityManager, new MongoMapperOptions(descriptionEntityManager));
		converters = new NoSQLCustomConverters(this, descriptionEntityManager.getDialect());
		this.addConverters(descriptionEntityManager);
	}

	public NoSQLEntityCache createEntityCache() {
		return getOptions().getCacheFactory().createCache();
	}

	public <T> T fromDocument(final NoSQLSession<?> session, final Class<T> entityClass, final Object dbDoc,
			final NoSQLEntityCache cache) throws Exception {
		if (dbDoc == null) {
			final Throwable t = new Throwable();
			LOG.error("A null reference was passed in for the dbObject", t);
			return null;
		}

		T entity;
		entity = options.getObjectFactory().createInstance(entityClass, dbDoc);
		entity = fromDocument(session, dbDoc, entity, cache);
		return entity;
	}

	<T> T fromDocument(final NoSQLSession<?> session, final Document dbObject) throws Exception {
		if (dbObject.containsKey(CLASS_NAME_FIELDNAME)) {
			T entity = options.getObjectFactory().createInstance(null, dbObject);
			entity = fromDocument(session, dbObject, entity, createEntityCache());

			return entity;
		} else {
			throw new NoSQLMappingException(
					format("The DBOBbject does not contain a %s key.  Determining entity type is impossible.",
							CLASS_NAME_FIELDNAME));
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T fromDocument(final NoSQLSession<?> session, final Object dbObject, final T entity, final NoSQLEntityCache cache)
			throws Exception {
		// hack to bypass things and just read the value.
		if (entity instanceof NoSQLDescriptionField) {
			readTypedDescriptionField(session, (NoSQLDescriptionField) entity, entity, cache, (DBObject) dbObject);
			return entity;
		}

		// check the history key (a key is the namespace + id)

		if (((Document) dbObject).containsKey(ID_KEY)
				&& descriptionEntityManager.getDescriptionEntity(entity.getClass()).getIdField() != null
				&& descriptionEntityManager.getDescriptionEntity(entity.getClass()) != null) {
			final Key<T> key = new Key(entity.getClass(),
					descriptionEntityManager.getDescriptionEntity(entity.getClass()).getCollectionName(),
					((Document) dbObject).get(ID_KEY));
			final T cachedInstance = cache.getEntity(key);
			if (cachedInstance != null) {
				return cachedInstance;
			} else {
				cache.putEntity(key, entity); // to avoid stackOverflow in recursive refs
			}
		}

		if (entity instanceof Map) {
			Map<String, Object> map = (Map<String, Object>) entity;
			for (String key : ((Document) dbObject).keySet()) {
				Object o = ((Document) dbObject).get(key);
				map.put(key, (o instanceof DBObject) ? fromDocument(session, (Document) o) : o);
			}
		} else if (entity instanceof Collection) {
			Collection<Object> collection = (Collection<Object>) entity;
			for (Object o : ((List<?>) dbObject)) {
				collection.add((o instanceof Document) ? fromDocument(session, (Document) o) : o);
			}
		} else {
			final NoSQLDescriptionEntity descriptionEntity = descriptionEntityManager
					.getDescriptionEntity(entity.getClass());
			final Document updated = (Document) descriptionEntity.callLifecycleMethods(BeforeLoad.class, entity, dbObject,
					this);
			try {
				for (final NoSQLDescriptionField descriptionField : descriptionEntity.getDescriptionFields()) {
					readTypedDescriptionField(session, descriptionField, entity, cache, updated);
				}
			} catch (final NoSQLMappingException e) {
				Object id = ((Document) dbObject).get(ID_KEY);
				String entityName = entity.getClass().getName();
				throw new NoSQLMappingException(format("Could not map %s with ID: %s in database '%s'", entityName, id,
						((MongoSession) session).getDatabase().getName()), e);
			}

			if (updated != null && updated.containsKey(ID_KEY)
					&& descriptionEntityManager.getDescriptionEntity(entity.getClass()).getIdField() != null) {
				final Key key = new Key(entity.getClass(),
						descriptionEntityManager.getDescriptionEntity(entity.getClass()).getCollectionName(),
						updated.get(ID_KEY));
				cache.putEntity(key, entity);
			}
			descriptionEntity.callLifecycleMethods(AfterLoad.class, entity, updated, this);
		}
		return entity;
	}

	public NoSQLConverters getConverters() {
		return converters;
	}

	public Object getId(final Object entity) {
		Object unwrapped = entity;
		if (unwrapped == null) {
			return null;
		}
		unwrapped = ProxyHelper.unwrap(unwrapped);
		try {
			return descriptionEntityManager.getDescriptionEntity(unwrapped.getClass()).getIdField().get(unwrapped);
		} catch (Exception e) {
			return null;
		}
	}

	public Map<Class<?>, Object> getInstanceCache() {
		return instanceCache;
	}

	@SuppressWarnings("unchecked")
	public <T> Key<T> getKey(final T entity) {
		T unwrapped = entity;
		if (unwrapped instanceof ProxiedEntityReference) {
			final ProxiedEntityReference proxy = (ProxiedEntityReference) unwrapped;
			return (Key<T>) proxy.__getKey();
		}

		unwrapped = ProxyHelper.unwrap(unwrapped);
		if (unwrapped instanceof Key) {
			return (Key<T>) unwrapped;
		}

		final Object id = getId(unwrapped);
		final Class<T> aClass = (Class<T>) unwrapped.getClass();
		return id == null ? null
				: new Key<T>(aClass, descriptionEntityManager.getDescriptionEntity(aClass).getCollectionName(), id);
	}

	@SuppressWarnings("unchecked")
	public <T> Key<T> getKey(final T entity, final String collection) {
		T unwrapped = entity;
		if (unwrapped instanceof ProxiedEntityReference) {
			final ProxiedEntityReference proxy = (ProxiedEntityReference) unwrapped;
			return (Key<T>) proxy.__getKey();
		}

		unwrapped = ProxyHelper.unwrap(unwrapped);
		if (unwrapped instanceof Key) {
			return (Key<T>) unwrapped;
		}

		final Object id = getId(unwrapped);
		final Class<T> aClass = (Class<T>) unwrapped.getClass();
		return id == null ? null : new Key<T>(aClass, collection, id);
	}

	public <T> List<Key<T>> getKeysByManualRefs(final Class<T> clazz, final List<Object> refs) {
		final String collection = descriptionEntityManager.getDescriptionEntity(clazz).getCollectionName();
		final List<Key<T>> keys = new ArrayList<Key<T>>(refs.size());
		for (final Object ref : refs) {
			keys.add(this.<T>manualRefToKey(collection, ref));
		}

		return keys;
	}

	public <T> List<Key<T>> getKeysByRefs(final List<Object> refs) {
		final List<Key<T>> keys = new ArrayList<Key<T>>(refs.size());
		for (final Object ref : refs) {
			final Key<T> testKey = refToKey((DBRef) ref);
			keys.add(testKey);
		}
		return keys;
	}

	public NoSQLMapperOptions getOptions() {
		return options;
	}

	public void setOptions(final NoSQLMapperOptions options) {
		this.options = options;
	}

	@Override
	public Object keyToDBRef(final Key<?> key) {
		if (key == null) {
			return null;
		}
		if (key.getType() == null && key.getCollection() == null) {
			throw new IllegalStateException("How can it be missing both?");
		}
		if (key.getCollection() == null) {
			key.setCollection(descriptionEntityManager.getDescriptionEntity(key.getType()).getCollectionName());
		}

		Object id = key.getId();
		if (descriptionEntityManager.isEntity(id.getClass())) {
			id = toMongoObject(id, true);
		}
		return new DBRef(key.getCollection(), id);
	}

	@Override
	public <T> Key<T> manualRefToKey(final Class<T> type, final Object id) {
		return id == null ? null
				: new Key<T>(type, descriptionEntityManager.getDescriptionEntity(type).getCollectionName(), id);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Key<T> refToKey(final Object ref) {
		return ref == null ? null
				: new Key<T>((Class<? extends T>) getClassFromCollection(((DBRef) ref).getCollectionName()),
						((DBRef) ref).getCollectionName(), ((DBRef) ref).getId());
	}

	public Document toDocument(final Object entity) {
		return toDocument(entity, null);
	}

	public Document toDocument(final Object entity, final Map<Object, Object> involvedObjects) {
		return toDocument(entity, involvedObjects, true);
	}

	@SuppressWarnings("deprecation")
	public Object toMongoObject(final NoSQLDescriptionField descriptionField,
			final NoSQLDescriptionEntity descriptionEntity, final Object value) {
		if (value == null) {
			return null;
		}

		Object mappedValue = value;

		if (isAssignable(descriptionField, value) || (descriptionEntity!=null && descriptionEntity.isEntity())) {
			// convert the value to Key (DBRef) if the field is @Reference or type is
			// Key/DBRef, or if the destination class is an @Entity
			try {
				if (value instanceof Iterable) {
					NoSQLDescriptionEntity subClassDescriptionEntity = descriptionEntityManager
							.getDescriptionEntity(descriptionField.getSubClass());
					if (subClassDescriptionEntity != null
							&& (Key.class.isAssignableFrom(subClassDescriptionEntity.getEntityClass()))) {
						mappedValue = getDBRefs(descriptionField, (Iterable<?>) value);
					} else {
						if (descriptionField.isReferenced()) {
							mappedValue = getDBRefs(descriptionField, (Iterable<?>) value);
						} else {
							mappedValue = toMongoObject(value, false);
						}
					}
				} else {
					if (descriptionField != null) {
						Class<?> idType = null;
						if (!descriptionField.getField().getType().equals(Key.class)
								&& isMapped(descriptionField.getField().getType())) {
							idType = descriptionEntityManager
									.getDescriptionEntity(descriptionField.getField().getType()).getDescriptionIdField()
									.getField().getType();
						}
						boolean valueIsIdType = mappedValue.getClass().equals(idType);
						if (descriptionField.isReferenced()) {
							if (!valueIsIdType) {
								Key<?> key = value instanceof Key ? (Key<?>) value : getKey(value);
								if (key != null) {
									mappedValue = descriptionField.isIdOnlyReference() ? keyToId(key) : keyToDBRef(key);
								}
							}
						} else if (descriptionField.getField().getType().equals(Key.class)) {
							mappedValue = keyToDBRef(valueIsIdType ? createKey(descriptionField.getSubClass(), value)
									: value instanceof Key ? (Key<?>) value : getKey(value));
							if (mappedValue == value) {
								throw new NoSQLMappingException("cannot map to Key<T> field: " + value);
							}
						}
					}

					if (mappedValue == value) {
						mappedValue = toMongoObject(value, false);
					}
				}
			} catch (Exception e) {
				LOG.error("Error converting value(" + value + ") to reference.", e);
				mappedValue = toMongoObject(value, false);
			}
		} else if (descriptionField != null && descriptionField.isSerialized()) { // serialized
			try {
				mappedValue = NoSQLSerializer.serialize(value, !descriptionField.isDisableCompression());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		} else if (value instanceof DBObject) { // pass-through
			mappedValue = value;
		} else {
			mappedValue = toMongoObject(value,
					MongoEmbeddedMapper.shouldSaveClassName(value, mappedValue, descriptionField));
			if (mappedValue instanceof BasicDBList) {
				final BasicDBList list = (BasicDBList) mappedValue;
				if (list.size() != 0) {
					if (!MongoEmbeddedMapper.shouldSaveClassName(extractFirstElement(value), list.get(0),
							descriptionField)) {
						for (Object o : list) {
							if (o instanceof DBObject) {
								((DBObject) o).removeField(CLASS_NAME_FIELDNAME);
							}
						}
					}
				}
			} else if (mappedValue instanceof DBObject
					&& !MongoEmbeddedMapper.shouldSaveClassName(value, mappedValue, descriptionField)) {
				((DBObject) mappedValue).removeField(CLASS_NAME_FIELDNAME);
			}
		}

		return mappedValue;
	}

	public String updateCollection(final Key key) {
		if (key.getCollection() == null && key.getType() == null) {
			throw new IllegalStateException("Key is invalid! " + toString());
		} else if (key.getCollection() == null) {
			key.setCollection(descriptionEntityManager.getDescriptionEntity(key.getType()).getCollectionName());
		}

		return key.getCollection();
	}

	public void updateKeyAndVersionInfo(final NoSQLSession<?> session, final Object dbObj, final NoSQLEntityCache cache,
			final Object entity) throws Exception {
		final NoSQLDescriptionEntity descriptionEntity = descriptionEntityManager
				.getDescriptionEntity(entity.getClass());

		if ((descriptionEntity.getIdField() != null) && (dbObj != null) && (((DBObject)dbObj).get(ID_KEY) != null)) {
			try {
				final NoSQLDescriptionField descriptionField = descriptionEntity.getDescriptionIdField();
				final Object oldIdValue = descriptionEntity.getIdField().get(entity);
				readTypedDescriptionField(session, descriptionField, entity, cache, dbObj);
				if (oldIdValue != null) {
					final Object dbIdValue = descriptionField.getObjectValue(entity);
					if (!dbIdValue.equals(oldIdValue)) {
						descriptionField.setObjectValue(entity, oldIdValue); // put the value back...
						throw new RuntimeException(format("@Id mismatch: %s != %s for %s", oldIdValue, dbIdValue,
								entity.getClass().getName()));
					}
				}
			} catch (Exception e) {
				if (e instanceof RuntimeException) {
					throw (RuntimeException) e;
				}

				throw new RuntimeException("Error setting @Id field after save/insert.", e);
			}
		}
		if (descriptionEntity.getDescriptionVersionField() != null && (dbObj != null)) {
			readTypedDescriptionField(session, descriptionEntity.getDescriptionVersionField(), entity, cache, dbObj);
		}
	}

	private void addConverters(NoSQLDescriptionEntityManager descriptionEntityManager) {
		for (NoSQLDescriptionEntity descriptionEntity : descriptionEntityManager.getDescriptionEntities()) {
			for (Class<? extends NoSQLTypeConverter> a : descriptionEntity.getConverters()) {
				if (!getConverters().isRegistered(a)) {
					getConverters().addConverter(a);
				}
			}
		}
	}

	private Object extractFirstElement(final Object value) {
		return value.getClass().isArray() ? Array.get(value, 0) : ((Iterable<?>) value).iterator().next();
	}

	private Object getDBRefs(final NoSQLDescriptionField descriptionField, final Iterable<?> value) {
		final List<Object> refs = new ArrayList<Object>();
		boolean idOnly = descriptionField.isIdOnlyReference();
		for (final Object o : value) {
			Key<?> key = (o instanceof Key) ? (Key<?>) o : getKey(o);
			refs.add(idOnly ? key.getId() : keyToDBRef(key));
		}
		return refs;
	}

	private boolean isAssignable(final NoSQLDescriptionField descriptionField, final Object value) {
		return descriptionField != null
				&& (descriptionField.isReferenced() || Key.class.isAssignableFrom(descriptionField.getField().getType())
						|| DBRef.class.isAssignableFrom(descriptionField.getField().getType())
						|| isMultiValued(descriptionField, value));

	}

	private boolean isMultiValued(final NoSQLDescriptionField descriptionField, final Object value) {
		final Class<?> subClass = descriptionField.getSubClass();
		return value instanceof Iterable && descriptionField.isAnyArrayOrCollection()
				&& (Key.class.isAssignableFrom(subClass) || DBRef.class.isAssignableFrom(subClass));
	}

	private void readTypedDescriptionField(final NoSQLSession<?> session, final NoSQLDescriptionField descriptionField,
			final Object entity, final NoSQLEntityCache cache, final Object dbObject) throws Exception {
		if (descriptionField.isProperty() || descriptionField.isSerialized()
				|| getConverters().hasSimpleValueConverter(descriptionField)) {
			options.getValueMapper().fromDocument(session, dbObject, descriptionField, entity, cache, this);
		} else if (descriptionField.isEmbedded()) {
			options.getEmbeddedMapper().fromDocument(session, dbObject, descriptionField, entity, cache, this);
		} else if (descriptionField.isReferenced()) {
			options.getReferenceMapper().fromDocument(session, dbObject, descriptionField, entity, cache, this);
		} else {
			options.getDefaultMapper().fromDocument(session, dbObject, descriptionField, entity, cache, this);
		}
	}

	private void writeMappedField(final Document dbObject, final NoSQLDescriptionField descriptionField,
			final Object entity, final Map<Object, Object> involvedObjects) throws Exception {

		if (descriptionField.isNotSaved()) {
			return;
		}

		if (descriptionField.isProperty() || descriptionField.isSerialized()
				|| (getConverters().hasSimpleValueConverter(descriptionField)
						|| (getConverters().hasSimpleValueConverter(descriptionField.getObjectValue(entity))))) {
			options.getValueMapper().toDocument(entity, descriptionField, dbObject, involvedObjects, this);
		} else if (descriptionField.isReferenced()) {
			options.getReferenceMapper().toDocument(entity, descriptionField, dbObject, involvedObjects, this);
		} else if (descriptionField.isEmbedded()) {
			options.getEmbeddedMapper().toDocument(entity, descriptionField, dbObject, involvedObjects, this);
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("No annotation was found, using default mapper " + options.getDefaultMapper() + " for "
						+ descriptionField);
			}
			options.getDefaultMapper().toDocument(entity, descriptionField, dbObject, involvedObjects, this);
		}

	}

	<T> Key<T> manualRefToKey(final String collection, final Object id) {
		return id == null ? null : new Key<T>((Class<? extends T>) getClassFromCollection(collection), collection, id);
	}

	public Object keyToId(final Key<?> key) {
		return key == null ? null : key.getId();
	}

	public Object toMongoObject(final Object javaObj, final boolean includeClassName) {
		if (javaObj == null) {
			return null;
		}
		Class<?> origClass = javaObj.getClass();

		if (origClass.isAnonymousClass() && origClass.getSuperclass().isEnum()) {
			origClass = origClass.getSuperclass();
		}

		final Object newObj = getConverters().encode(origClass, javaObj);
		if (newObj == null) {
			LOG.log(LogLevel.WARN, "converted " + javaObj + " to null");
			return null;
		}
		final Class<?> type = newObj.getClass();
		final boolean bSameType = origClass.equals(type);

		if (!bSameType && !(Map.class.isAssignableFrom(type) || Iterable.class.isAssignableFrom(type))) {
			return newObj;
		} else {
			boolean isSingleValue = true;
			boolean isMap = false;
			Class<?> subType = null;

			if (type.isArray() || Map.class.isAssignableFrom(type) || Iterable.class.isAssignableFrom(type)) {
				isSingleValue = false;
				isMap = ReflectionUtils.isImplementsInterface(type, Map.class);
				subType = (type.isArray()) ? type.getComponentType()
						: ReflectionUtils.getParameterizedClass(type, (isMap) ? 1 : 0);
			}

			if (isSingleValue && !isPropertyType(type)) {
				final Document dbDoc = toDocument(newObj);
				if (!includeClassName) {
					dbDoc.remove(CLASS_NAME_FIELDNAME);
				}
				return dbDoc;
			} else if (newObj instanceof DBObject) {
				return newObj;
			} else if (isMap) {
				if (isPropertyType(subType)) {
					return toDocument(newObj);
				} else {
					final HashMap m = new HashMap();
					for (final Map.Entry e : (Iterable<Map.Entry>) ((Map) newObj).entrySet()) {
						m.put(e.getKey(), toMongoObject(e.getValue(), includeClassName));
					}

					return m;
				}
				// Set/List but needs elements converted
			} else if (!isSingleValue && !isPropertyType(subType)) {
				final List<Object> values = new BasicDBList();
				if (type.isArray()) {
					for (final Object obj : (Object[]) newObj) {
						values.add(toMongoObject(obj, includeClassName));
					}
				} else {
					for (final Object obj : (Iterable<?>) newObj) {
						values.add(toMongoObject(obj, includeClassName));
					}
				}

				return values;
			} else {
				return newObj;
			}
		}
	}

	public Document toDocument(final Object entity, final Map<Object, Object> involvedObjects,
			final boolean lifecycle) {

		Document dbDoc = new Document();
		final NoSQLDescriptionEntity descriptionEntity = descriptionEntityManager
				.getDescriptionEntity(entity.getClass());

		if (!descriptionEntity.isNoClassnameStored()) {
			dbDoc.put(CLASS_NAME_FIELDNAME, entity.getClass().getName());
		}

		if (lifecycle) {
			dbDoc = (Document) descriptionEntity.callLifecycleMethods(BeforeSave.class, entity, dbDoc, this);
		}

		for (final NoSQLDescriptionField descriptionField : descriptionEntity.getDescriptionFields()) {
			try {
				writeMappedField(dbDoc, descriptionField, entity, involvedObjects);
			} catch (Exception e) {
				throw new NoSQLMappingException("Error mapping field:" + descriptionField.getFullName(), e);
			}
		}
		if (involvedObjects != null) {
			involvedObjects.put(entity, dbDoc);
		}

		if (lifecycle) {
			descriptionEntity.callLifecycleMethods(BeforeSave.class, entity, dbDoc, this);
		}

		return dbDoc;
	}

	public <T> Key<T> createKey(final Class<T> clazz, final Serializable id) {
		return new Key<T>(clazz, descriptionEntityManager.getDescriptionEntity(clazz).getCollectionName(), id);
	}

	public <T> Key<T> createKey(final Class<T> clazz, final Object id) {
		if (id instanceof Serializable) {
			return createKey(clazz, (Serializable) id);
		}

		Document documentId = toDocument(id);
		
		return new Key<T>(clazz, descriptionEntityManager.getDescriptionEntity(clazz).getCollectionName(),
				RawBsonDocument.parse(documentId.toJson()).asBinary().getData());
	}

	@Override
	public boolean isMapped(final Class<?> clazz) {
		return descriptionEntityManager.isEntity(clazz);
	}

	public static boolean isPropertyType(final Class<?> type) {
		return type != null && (isPrimitiveLike(type) || type == DBRef.class || type == Pattern.class
				|| type == CodeWScope.class || type == ObjectId.class || type == Key.class || type == DBObject.class || type == Document.class
				|| type == BasicDBObject.class);

	}

	public static boolean isPrimitiveLike(final Class<?> type) {
		return type != null && (type == String.class || type == char.class || type == Character.class
				|| type == short.class || type == Short.class || type == Integer.class || type == int.class
				|| type == Long.class || type == long.class || type == Double.class || type == double.class
				|| type == float.class || type == Float.class || type == Boolean.class || type == boolean.class
				|| type == Byte.class || type == byte.class || type == Date.class || type == Locale.class
				|| type == Class.class || type == UUID.class || type == URI.class || type.isEnum());

	}


}
