package br.com.anteros.nosql.persistence.mongodb.mapping;

import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import br.com.anteros.core.utils.ListUtils;
import br.com.anteros.core.utils.ObjectUtils;
import br.com.anteros.nosql.persistence.converters.IterHelper;
import br.com.anteros.nosql.persistence.converters.IterHelper.MapIterCallback;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;
import br.com.anteros.nosql.persistence.metadata.NoSQLTypedDescriptionField;
import br.com.anteros.nosql.persistence.session.NoSQLSession;
import br.com.anteros.nosql.persistence.session.cache.NoSQLEntityCache;
import br.com.anteros.nosql.persistence.session.mapping.AbstractNoSQLObjectMapper;
import br.com.anteros.nosql.persistence.session.mapping.NoSQLCustomMapper;

class MongoEmbeddedMapper implements NoSQLCustomMapper {
	static boolean shouldSaveClassName(final Object rawVal, final Object convertedVal,
			final NoSQLDescriptionField descriptionField) {
		if (rawVal == null || descriptionField == null) {
			return true;
		}
		if (descriptionField.isSimple()) {
			return !(descriptionField.getField().getType().equals(rawVal.getClass())
					&& !(convertedVal instanceof BasicDBList));
		}
		boolean isDocument = convertedVal instanceof Document;
		boolean anInterface = descriptionField.getSubClass().isInterface();
		boolean anAbstract = Modifier.isAbstract(descriptionField.getSubClass().getModifiers());
		boolean equals = descriptionField.getSubClass().equals(rawVal.getClass());
		return convertedVal == null || !isDocument || anInterface || anAbstract || !equals;
	}

	private static boolean isMapOrCollection(final NoSQLDescriptionField descriptionField) {
		return Map.class.isAssignableFrom(descriptionField.getSubClass())
				|| Iterable.class.isAssignableFrom(descriptionField.getSubClass());
	}

	@Override
	public void fromDocument(final NoSQLSession<?> session, final Object dbObject,
			final NoSQLDescriptionField descriptionField, final Object entity, final NoSQLEntityCache cache,
			final AbstractNoSQLObjectMapper mapper) throws Exception {
		try {
			if (descriptionField.isAnyMap()) {
				readMap(session, mapper, entity, cache, descriptionField, (Document) dbObject);
			} else if (descriptionField.isAnyArrayOrCollection()) {
				readCollection(session, mapper, entity, cache, descriptionField, (Document) dbObject);
			} else {
				// single element
				final Object dbVal = descriptionField.getDbObjectValue(session.getDialect(), dbObject);
				
				if (dbVal != null) {
					final boolean isDocument = dbVal instanceof Document;

					// run converters
					if (isDocument && !mapper.isMapped(descriptionField.getConcreteType())
							&& (mapper.getConverters().hasDocumentConverter(descriptionField) || mapper.getConverters()
									.hasDocumentConverter(descriptionField.getField().getType()))) {
						mapper.getConverters().fromDocument(dbObject, descriptionField, entity);
					} else {
						Object refObj;
						if (mapper.getConverters().hasSimpleValueConverter(descriptionField) || mapper.getConverters()
								.hasSimpleValueConverter(descriptionField.getField().getType())) {
							refObj = mapper.getConverters().decode(descriptionField.getField().getType(), dbVal,
									descriptionField);
						} else {
							Document value = (dbVal instanceof DBObject?new Document(((DBObject)dbVal).toMap()):(Document) dbVal);
							refObj = mapper.getOptions().getObjectFactory().createInstance(mapper, descriptionField,
									value);
							refObj = mapper.fromDocument(session, value, refObj, cache);
						}
						if (refObj != null) {
							descriptionField.setObjectValue(entity, refObj);
						}
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void toDocument(final Object entity, final NoSQLDescriptionField descriptionField, final Object dbObject,
			final Map<Object, Object> involvedObjects, final AbstractNoSQLObjectMapper mapper) throws Exception {
		final String name = descriptionField.getName();

		final Object fieldValue = descriptionField.getObjectValue(entity);

		if (descriptionField.isAnyMap()) {
			writeMap(descriptionField, (Document) dbObject, involvedObjects, name, fieldValue, mapper);
		} else if (descriptionField.isAnyArrayOrCollection()) {
			writeCollection(descriptionField, (Document) dbObject, involvedObjects, name, fieldValue, mapper);
		} else {
			// run converters
			if (mapper.getConverters().hasDocumentConverter(descriptionField)
					|| mapper.getConverters().hasDocumentConverter(entity.getClass())) {
				mapper.getConverters().toDocument(entity, descriptionField, dbObject, mapper.getOptions());
				return;
			}

			final Document dbObj = fieldValue == null ? null
					: (Document) mapper.toDocument(fieldValue, involvedObjects);
			if (dbObj != null) {
				if (!shouldSaveClassName(fieldValue, dbObj, descriptionField)) {
					dbObj.remove(AbstractNoSQLObjectMapper.CLASS_NAME_FIELDNAME);
				}

				if (!dbObj.keySet().isEmpty() || mapper.getOptions().isStoreEmpties()) {
					((Document) dbObject).put(name, dbObj);
				}
			}
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void readCollection(final NoSQLSession session, final AbstractNoSQLObjectMapper mapper, final Object entity,
			final NoSQLEntityCache cache, final NoSQLDescriptionField descriptionField, final Document dbObject)
			throws Exception {
		Collection values;

		final Object dbVal = descriptionField.getDbObjectValue(session.getDialect(), dbObject);
		if (dbVal != null) {
			// multiple documents in a List
			values = descriptionField.isAnyArrayOrCollection()
					? mapper.getOptions().getObjectFactory().createSet(descriptionField)
					: mapper.getOptions().getObjectFactory().createList(descriptionField);

			final List dbValues;
			if (dbVal instanceof List) {
				dbValues = (List) dbVal;
			} else {
				dbValues = new BasicDBList();
				dbValues.add(dbVal);
			}

			NoSQLTypedDescriptionField typedDescriptionField = !mapper.isMapped(descriptionField.getField().getType())
					&& isMapOrCollection(descriptionField)
					&& (descriptionField.getSubType() instanceof ParameterizedType)
							? new NoSQLTypedDescriptionField(
									(ParameterizedType) descriptionField.getSubType(), descriptionField)
							: null;
			for (final Object o : dbValues) {

				Object newEntity = null;

				if (o != null) {
					// run converters
					if (mapper.getConverters().hasSimpleValueConverter(descriptionField)
							|| mapper.getConverters().hasSimpleValueConverter(descriptionField.getSubClass())) {
						newEntity = mapper.getConverters().decode(descriptionField.getSubClass(), o, descriptionField);
					} else {
						newEntity = readMapOrCollectionOrEntity(session, mapper, cache, descriptionField,
								typedDescriptionField, (Document) o);
					}
				}

				values.add(newEntity);
			}
			if (!values.isEmpty() || mapper.getOptions().isStoreEmpties()) {
				if (descriptionField.isArray()) {
					descriptionField.setObjectValue(entity,
							ListUtils.convertToArray(descriptionField.getSubClass(), ListUtils.iterToList(values)));
				} else {
					if (descriptionField.isList()) {
						descriptionField.setObjectValue(entity,new ArrayList(values));	
					} else {
						descriptionField.setObjectValue(entity, values);
					}
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void readMap(final NoSQLSession<?> session, final AbstractNoSQLObjectMapper mapper, final Object entity,
			final NoSQLEntityCache cache, final NoSQLDescriptionField descriptionField, final Document dbObject)
			throws Exception {
		final Document dbObj = (Document) descriptionField.getDbObjectValue(session.getDialect(), dbObject);

		if (dbObj != null) {
			final Map map = mapper.getOptions().getObjectFactory().createMap(descriptionField);

			final NoSQLTypedDescriptionField typedDescriptionField = isMapOrCollection(descriptionField)
					? new NoSQLTypedDescriptionField((ParameterizedType) descriptionField.getSubType(),
							descriptionField)
					: null;
			new IterHelper<Object, Object>().loopMap(dbObj, new MapIterCallback<Object, Object>() {
				@Override
				public void eval(final Object k, final Object val) {
					Object newEntity = null;

					// run converters
					if (val != null) {
						if (mapper.getConverters().hasSimpleValueConverter(descriptionField)
								|| mapper.getConverters().hasSimpleValueConverter(descriptionField.getSubClass())) {
							newEntity = mapper.getConverters().decode(descriptionField.getSubClass(), val,
									descriptionField);
						} else {
							if (val instanceof Document) {
								try {
									newEntity = readMapOrCollectionOrEntity(session, mapper, cache, descriptionField,
											typedDescriptionField, (Document) val);
								} catch (Exception e) {
									e.printStackTrace();
								}
							} else {
								newEntity = val;
							}

						}
					}

					final Object objKey = mapper.getConverters().decode(descriptionField.getMapKeyClass(), k,
							descriptionField);
					map.put(objKey, newEntity);
				}
			});

			if (!map.isEmpty() || mapper.getOptions().isStoreEmpties()) {
				descriptionField.setObjectValue(entity, map);
			}
		}
	}

	private Object readMapOrCollectionOrEntity(final NoSQLSession<?> session, final AbstractNoSQLObjectMapper mapper,
			final NoSQLEntityCache cache, final NoSQLDescriptionField descriptionField,
			final NoSQLTypedDescriptionField typedField, final Document dbObj) throws Exception {
		if (typedField != null) {
			mapper.fromDocument(session, dbObj, typedField, cache);
			return typedField.getValue();
		} else {
			final Object newEntity = mapper.getOptions().getObjectFactory().createInstance(mapper, descriptionField,
					dbObj);
			return mapper.fromDocument(session, dbObj, newEntity, cache);
		}
	}

	private void writeCollection(final NoSQLDescriptionField descriptionField, final Document dbObject,
			final Map<Object, Object> involvedObjects, final String name, final Object fieldValue,
			final AbstractNoSQLObjectMapper mapper) {
		Iterable<?> coll = null;

		if (fieldValue != null) {
			if (descriptionField.isArray()) {
				coll = Arrays.asList((Object[]) fieldValue);
			} else {
				coll = (Iterable<?>) fieldValue;
			}
		}

		if (coll != null) {
			final List<Object> values = new ArrayList<Object>();
			for (final Object o : coll) {
				if (null == o) {
					values.add(null);
				} else if (mapper.getConverters().hasSimpleValueConverter(descriptionField)
						|| mapper.getConverters().hasSimpleValueConverter(o.getClass())) {
					values.add(mapper.getConverters().encode(o));
				} else {
					final Object val;
					if (Collection.class.isAssignableFrom(o.getClass()) || Map.class.isAssignableFrom(o.getClass())) {
						val = ((MongoObjectMapper) mapper).toMongoObject(o, true);
					} else {
						val = mapper.toDocument(o, involvedObjects);
					}

					if (!shouldSaveClassName(o, val, descriptionField)) {
						((Document) val).remove(AbstractNoSQLObjectMapper.CLASS_NAME_FIELDNAME);
					}

					values.add(val);
				}
			}
			if (!values.isEmpty() || mapper.getOptions().isStoreEmpties()) {
				dbObject.put(name, values);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void writeMap(final NoSQLDescriptionField descriptionField, final Document dbObject,
			final Map<Object, Object> involvedObjects, final String name, final Object fieldValue,
			final AbstractNoSQLObjectMapper mapper) {
		final Map<String, Object> map = (Map<String, Object>) fieldValue;
		if (map != null) {
			final Document values = new Document();

			for (final Map.Entry<String, Object> entry : map.entrySet()) {
				final Object entryVal = entry.getValue();
				final Object val;

				if (entryVal == null) {
					val = null;
				} else if (mapper.getConverters().hasSimpleValueConverter(descriptionField)
						|| mapper.getConverters().hasSimpleValueConverter(entryVal.getClass())) {
					val = mapper.getConverters().encode(entryVal);
				} else {
					if (Map.class.isAssignableFrom(entryVal.getClass())
							|| Collection.class.isAssignableFrom(entryVal.getClass())) {
						val = ((MongoObjectMapper) mapper).toMongoObject(entryVal, true);
					} else {
						val = mapper.toDocument(entryVal, involvedObjects);
					}

					if (!shouldSaveClassName(entryVal, val, descriptionField)) {
						if (val instanceof List) {
							if (((List<?>) val).get(0) instanceof Document) {
								List<Document> list = (List<Document>) val;
								for (Document o : list) {
									o.remove(AbstractNoSQLObjectMapper.CLASS_NAME_FIELDNAME);
								}
							}
						} else {
							((Document) val).remove(AbstractNoSQLObjectMapper.CLASS_NAME_FIELDNAME);
						}
					}
				}

				final String strKey = mapper.getConverters().encode(entry.getKey()).toString();
				values.put(strKey, val);
			}

			if (!values.isEmpty() || mapper.getOptions().isStoreEmpties()) {
				dbObject.put(name, values);
			}
		}
	}

}
