package br.com.anteros.nosql.persistence.mongodb.mapping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.DBCollectionFindOptions;

import br.com.anteros.core.log.LogLevel;
import br.com.anteros.core.log.Logger;
import br.com.anteros.core.log.LoggerProvider;
import br.com.anteros.core.utils.ListUtils;
import br.com.anteros.nosql.persistence.converters.IterHelper;
import br.com.anteros.nosql.persistence.converters.Key;
import br.com.anteros.nosql.persistence.converters.NoSQLMappingException;
import br.com.anteros.nosql.persistence.converters.IterHelper.IterCallback;
import br.com.anteros.nosql.persistence.converters.IterHelper.MapIterCallback;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionEntity;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;
import br.com.anteros.nosql.persistence.mongodb.session.MongoSession;
import br.com.anteros.nosql.persistence.proxy.LazyFeatureDependencies;
import br.com.anteros.nosql.persistence.proxy.ProxiedEntityReference;
import br.com.anteros.nosql.persistence.proxy.ProxiedEntityReferenceList;
import br.com.anteros.nosql.persistence.proxy.ProxiedEntityReferenceMap;
import br.com.anteros.nosql.persistence.proxy.ProxyHelper;
import br.com.anteros.nosql.persistence.session.NoSQLSession;
import br.com.anteros.nosql.persistence.session.cache.NoSQLEntityCache;
import br.com.anteros.nosql.persistence.session.mapping.AbstractNoSQLObjectMapper;
import br.com.anteros.nosql.persistence.session.mapping.NoSQLCustomMapper;

@SuppressWarnings({ "unchecked", "rawtypes" })
class MongoReferenceMapper implements NoSQLCustomMapper {
	private static Logger LOG = LoggerProvider.getInstance().getLogger(MongoReferenceMapper.class);

	@Override
	public void fromDocument(final NoSQLSession session, final Object dbObject,
			final NoSQLDescriptionField descriptionField, final Object entity, final NoSQLEntityCache cache,
			final AbstractNoSQLObjectMapper mapper) throws Exception {
		final Class fieldType = descriptionField.getField().getType();

		if (descriptionField.isAnyMap()) {
			readMap(session, mapper, entity, cache, descriptionField, (Document) dbObject);
		} else if (descriptionField.isAnyArrayOrCollection()) {
			readCollection(session, mapper, (Document) dbObject, descriptionField, entity, cache);
		} else {
			readSingle(session, mapper, entity, fieldType, cache, descriptionField, (Document) dbObject);
		}

	}

	@Override
	public void toDocument(final Object entity, final NoSQLDescriptionField descriptionField, final Object dbObject,
			final Map<Object, Object> involvedObjects, final AbstractNoSQLObjectMapper mapper) throws Exception {
		final String name = descriptionField.getName();

		final Object fieldValue = descriptionField.getObjectValue(entity);

		if (fieldValue == null && !mapper.getOptions().isStoreNulls()) {
			return;
		}

		if (descriptionField.isAnyMap()) {
			writeMap(descriptionField, (Document) dbObject, name, fieldValue, mapper);
		} else if (descriptionField.isAnyArrayOrCollection()) {
			writeCollection(descriptionField, (Document) dbObject, name, fieldValue, mapper);
		} else {
			writeSingle((Document) dbObject, name, fieldValue, mapper, descriptionField);
		}

	}

	private void addValue(final List values, final Object o, final AbstractNoSQLObjectMapper mapper,
			final boolean idOnly) {
		if (o == null && mapper.getOptions().isStoreNulls()) {
			values.add(null);
			return;
		}

		final Key key = o instanceof Key ? (Key) o : getKey(o, mapper);
		values.add(idOnly ? mapper.keyToId(key) : mapper.keyToDBRef(key));
	}

	private Object createOrReuseProxy(final NoSQLSession session, final AbstractNoSQLObjectMapper mapper,
			final Class referenceObjClass, final Object ref, final NoSQLEntityCache cache,
			final NoSQLDescriptionField descriptionField) {
		final Key key = descriptionField.isIdOnlyReference() ? mapper.manualRefToKey(referenceObjClass, ref)
				: mapper.refToKey((DBRef) ref);
		final Object proxyAlreadyCreated = cache.getProxy(key);
		if (proxyAlreadyCreated != null) {
			return proxyAlreadyCreated;
		}
		final Object newProxy = mapper.getProxyFactory().createProxy(session, referenceObjClass, key,
				descriptionField.isIgnoreMissingReference());
		cache.putProxy(key, newProxy);
		return newProxy;
	}

	private Key<?> getKey(final Object entity, final AbstractNoSQLObjectMapper mapper) {
		try {
			if (entity instanceof ProxiedEntityReference) {
				final ProxiedEntityReference proxy = (ProxiedEntityReference) entity;
				return proxy.__getKey();
			}
			final NoSQLDescriptionEntity mappedClass = mapper.getDescriptionEntityManager()
					.getDescriptionEntity(entity.getClass());
			Object id = mappedClass.getIdField().get(entity);
			if (id == null) {
				throw new NoSQLMappingException("@Id field cannot be null!");
			}
			return new Key(mappedClass.getEntityClass(), mappedClass.getCollectionName(), id);
		} catch (IllegalAccessException iae) {
			throw new RuntimeException(iae);
		}
	}

	private void readCollection(final NoSQLSession session, final AbstractNoSQLObjectMapper mapper,
			final Document dbObject, final NoSQLDescriptionField descriptionField, final Object entity,
			final NoSQLEntityCache cache) throws Exception {
		// multiple references in a List
		final Class referenceObjClass = descriptionField.getSubClass();
		// load reference class. this "fixes" #816
		mapper.getDescriptionEntityManager().getDescriptionEntity(referenceObjClass);
		Collection references = descriptionField.isAnyArrayOrCollection()
				? mapper.getOptions().getObjectFactory().createSet(descriptionField)
				: mapper.getOptions().getObjectFactory().createList(descriptionField);

		if (descriptionField.isLazyLoadReference() && LazyFeatureDependencies.assertDependencyFullFilled()) {
			final Object dbVal = descriptionField.getDbObjectValue(session.getDialect(), dbObject);
			if (dbVal != null) {
				references = mapper.getProxyFactory().createListProxy(session, references, referenceObjClass,
						descriptionField.isIgnoreMissingReference());
				final ProxiedEntityReferenceList referencesAsProxy = (ProxiedEntityReferenceList) references;

				if (dbVal instanceof List) {
					referencesAsProxy.__addAll(descriptionField.isIdOnlyReference()
							? mapper.getKeysByManualRefs(referenceObjClass, (List) dbVal)
							: mapper.getKeysByRefs((List) dbVal));
				} else {
					referencesAsProxy.__add(
							descriptionField.isIdOnlyReference() ? mapper.manualRefToKey(referenceObjClass, dbVal)
									: mapper.refToKey((DBRef) dbVal));
				}
			}
		} else {
			final Object dbVal = descriptionField.getDbObjectValue(session.getDialect(), dbObject);
			final Collection refs = references;
			new IterHelper<String, Object>().loopOrSingle(dbVal, new IterCallback<Object>() {
				@Override
				public void eval(final Object val) {
					Object ent = null;
					try {
						ent = resolveObject(session, mapper, cache, descriptionField,
								descriptionField.isIdOnlyReference(), val);
					} catch (Exception e) {
						e.printStackTrace();
					}
					if (ent == null) {
						LOG.log(LogLevel.WARN,
								"Null reference found when retrieving value for " + descriptionField.getFullName());
					} else {
						refs.add(ent);
					}
				}
			});
		}

		if (descriptionField.isArray()) {
			descriptionField.setObjectValue(entity,
					ListUtils.convertToArray(descriptionField.getSubClass(), ListUtils.iterToList(references)));
		} else {
			if (descriptionField.isList()) {
				descriptionField.setObjectValue(entity, new ArrayList<>(references));
			} else {
				descriptionField.setObjectValue(entity, references);
			}
		}
	}

	private void readMap(final NoSQLSession session, final AbstractNoSQLObjectMapper mapper, final Object entity,
			final NoSQLEntityCache cache, final NoSQLDescriptionField descriptionField, final Document dbObject)
			throws Exception {
		final Class referenceObjClass = descriptionField.getSubClass();
		Map m = mapper.getOptions().getObjectFactory().createMap(descriptionField);

		final DBObject dbVal = (DBObject) descriptionField.getDbObjectValue(session.getDialect(), dbObject);
		if (dbVal != null) {
			if (descriptionField.isLazyLoadReference() && LazyFeatureDependencies.assertDependencyFullFilled()) {
				// replace map by proxy to it.
				m = mapper.getProxyFactory().createMapProxy(session, m, referenceObjClass,
						descriptionField.isIgnoreMissingReference());
			}

			final Map map = m;
			new IterHelper<Object, Object>().loopMap(dbVal, new MapIterCallback<Object, Object>() {
				@Override
				public void eval(final Object k, final Object val) {

					final Object objKey = mapper.getConverters().decode(descriptionField.getMapKeyClass(), k,
							descriptionField);

					if (descriptionField.isLazyLoadReference()
							&& LazyFeatureDependencies.assertDependencyFullFilled()) {
						final ProxiedEntityReferenceMap proxiedMap = (ProxiedEntityReferenceMap) map;
						proxiedMap.__put(objKey,
								descriptionField.isIdOnlyReference() ? mapper.manualRefToKey(referenceObjClass, val)
										: mapper.refToKey((DBRef) val));
					} else {
						Object value;
						try {
							value = resolveObject(session, mapper, cache, descriptionField,
									descriptionField.isIdOnlyReference(), val);
							map.put(objKey, value);
						} catch (Exception e) {
							e.printStackTrace();
						}

					}
				}
			});
		}
		descriptionField.setObjectValue(entity, m);
	}

	private void readSingle(final NoSQLSession session, final AbstractNoSQLObjectMapper mapper, final Object entity,
			final Class fieldType, final NoSQLEntityCache cache, final NoSQLDescriptionField descriptionField,
			final Document dbObject) throws Exception {

		final Object ref = descriptionField.getDbObjectValue(session.getDialect(), dbObject);
		if (ref != null) {
			Object resolvedObject;
			if (descriptionField.isLazyLoadReference() && LazyFeatureDependencies.assertDependencyFullFilled()) {
				resolvedObject = createOrReuseProxy(session, mapper, fieldType, ref, cache, descriptionField);
			} else {
				resolvedObject = resolveObject(session, mapper, cache, descriptionField,
						descriptionField.isIdOnlyReference(), ref);
			}

			if (resolvedObject != null) {
				descriptionField.setObjectValue(entity, resolvedObject);
			}

		}
	}

	private void writeCollection(final NoSQLDescriptionField descriptionField, final Document dbObject,
			final String name, final Object fieldValue, final AbstractNoSQLObjectMapper mapper) {
		if (fieldValue != null) {
			final List values = new ArrayList();

			if (ProxyHelper.isProxy(fieldValue) && ProxyHelper.isUnFetched(fieldValue)) {
				final ProxiedEntityReferenceList p = (ProxiedEntityReferenceList) fieldValue;
				final List<Key<?>> getKeysAsList = p.__getKeysAsList();
				for (final Key<?> key : getKeysAsList) {
					addValue(values, key, mapper, descriptionField.isIdOnlyReference());
				}
			} else {

				if (descriptionField.isArray()) {
					for (final Object o : (Object[]) fieldValue) {
						addValue(values, o, mapper, descriptionField.isIdOnlyReference());
					}
				} else {
					for (final Object o : (Iterable) fieldValue) {
						addValue(values, o, mapper, descriptionField.isIdOnlyReference());
					}
				}
			}
			if (!values.isEmpty() || mapper.getOptions().isStoreEmpties()) {
				dbObject.put(name, values);
			}
		}
	}

	private void writeMap(final NoSQLDescriptionField descriptionField, final Document dbObject, final String name,
			final Object fieldValue, final AbstractNoSQLObjectMapper mapper) {
		final Map<Object, Object> map = (Map<Object, Object>) fieldValue;
		if ((map != null)) {
			final Map values = mapper.getOptions().getObjectFactory().createMap(descriptionField);

			if (ProxyHelper.isProxy(map) && ProxyHelper.isUnFetched(map)) {
				final ProxiedEntityReferenceMap proxy = (ProxiedEntityReferenceMap) map;

				final Map<Object, Key<?>> refMap = proxy.__getReferenceMap();
				for (final Map.Entry<Object, Key<?>> entry : refMap.entrySet()) {
					final Object key = entry.getKey();
					values.put(key, descriptionField.isIdOnlyReference() ? mapper.keyToId(entry.getValue())
							: mapper.keyToDBRef(entry.getValue()));
				}
			} else {
				for (final Map.Entry<Object, Object> entry : map.entrySet()) {
					final String strKey = mapper.getConverters().encode(entry.getKey()).toString();
					values.put(strKey,
							descriptionField.isIdOnlyReference() ? mapper.keyToId(getKey(entry.getValue(), mapper))
									: mapper.keyToDBRef(getKey(entry.getValue(), mapper)));
				}
			}
			if (!values.isEmpty() || mapper.getOptions().isStoreEmpties()) {
				dbObject.put(name, values);
			}
		}
	}

	private void writeSingle(final Document dbObject, final String name, final Object fieldValue,
			final AbstractNoSQLObjectMapper mapper, NoSQLDescriptionField descriptionField) {
		if (fieldValue == null) {
			if (mapper.getOptions().isStoreNulls()) {
				dbObject.put(name, null);
			}
		} else {
			Key<?> key = getKey(fieldValue, mapper);
			if (descriptionField.isIdOnlyReference()) {
				Object id = mapper.keyToId(key);
				if (id != null && mapper.isMapped(id.getClass())) {
					id = ((MongoObjectMapper) mapper).toMongoObject(id, true);
				}

				dbObject.put(name, id);
			} else {
				dbObject.put(name, mapper.keyToDBRef(key));
			}
		}
	}

	Object resolveObject(final NoSQLSession session, final AbstractNoSQLObjectMapper mapper,
			final NoSQLEntityCache cache, final NoSQLDescriptionField descriptionField, final boolean idOnly,
			final Object ref) throws Exception {
		if (ref == null) {
			return null;
		}

		final DBRef dbRef = idOnly ? null : (DBRef) ref;

		Class<?> type = descriptionField.isSimple() ? descriptionField.getField().getType()
				: descriptionField.getSubClass();

		final Key key = mapper.createKey(type, idOnly ? ref : ((DBRef) dbRef).getId());

		final Object cached = cache.getEntity(key);
		if (cached != null) {
			return cached;
		}

		final Document refDbObject;
		MongoCollection<Document> collection;
		Object id;

		if (idOnly) {
			collection = ((MongoSession) session).getCollection(key.getType());
			id = ref;
		} else {
			MongoSession mongoSession = ((MongoSession) session);
			collection = mongoSession.getDatabase().getCollection(dbRef.getCollectionName());
			id = dbRef.getId();
		}
		if (id instanceof Document) {
			((Document) id).remove(AbstractNoSQLObjectMapper.CLASS_NAME_FIELDNAME);
		}
		
		Document searchDocument = new Document();
	    searchDocument.append(AbstractNoSQLObjectMapper.ID_KEY, id);
	    
		FindIterable<Document> iterable = collection.find(searchDocument);
		refDbObject = iterable.first();
		
		if (refDbObject != null) {
			Object refObj = mapper.getOptions().getObjectFactory().createInstance(mapper, descriptionField,
					refDbObject);
			refObj = mapper.fromDocument(session, refDbObject, refObj, cache);
			cache.putEntity(key, refObj);
			return refObj;
		}

		final boolean ignoreMissing = descriptionField.isIgnoreMissingReference();
		if (!ignoreMissing) {
			throw new NoSQLMappingException(
					"The reference(" + ref.toString() + ") could not be fetched for " + descriptionField.getFullName());
		} else {
			return null;
		}
	}
}
