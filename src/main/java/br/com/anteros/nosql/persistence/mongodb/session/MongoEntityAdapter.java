package br.com.anteros.nosql.persistence.mongodb.session;

import java.util.LinkedHashMap;

import org.bson.Document;
import org.bson.types.ObjectId;

import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionEntity;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;
import br.com.anteros.nosql.persistence.mongodb.mapping.MongoObjectMapper;
import br.com.anteros.nosql.persistence.mongodb.query.MongoCriteria;
import br.com.anteros.nosql.persistence.mongodb.query.MongoQuery;
import br.com.anteros.nosql.persistence.proxy.ProxyHelper;
import br.com.anteros.nosql.persistence.session.NoSQLEntityAdapter;
import br.com.anteros.nosql.persistence.session.NoSQLMappedDocument;
import br.com.anteros.nosql.persistence.session.mapping.AbstractNoSQLObjectMapper;
import br.com.anteros.nosql.persistence.session.query.NoSQLQuery;

public class MongoEntityAdapter<T> implements NoSQLEntityAdapter<T> {

	private T entity;
	private NoSQLDescriptionEntity descriptionEntity;
	private AbstractNoSQLObjectMapper mapper;

	private MongoEntityAdapter(T entity, NoSQLDescriptionEntity descriptionEntity, AbstractNoSQLObjectMapper mapper) {
		this.entity = entity;
		this.descriptionEntity = descriptionEntity;
		this.mapper = mapper;
	}

	public static <T, Q> MongoEntityAdapter<T> of(T entity, NoSQLDescriptionEntity descriptionEntity,
			AbstractNoSQLObjectMapper mapper) {
		return new MongoEntityAdapter<>(entity, descriptionEntity, mapper);
	}

	@Override
	public String getIdFieldName() {
		return descriptionEntity.getDescriptionIdField().getName();
	}

	@Override
	public Object getId() {
		return descriptionEntity.getDescriptionIdField().getObjectValue(entity);
	}

	@Override
	public NoSQLQuery getByIdQuery() {
		Object newId = getId();
		if (newId instanceof String) {
			newId = new ObjectId(newId.toString());
		}
		return MongoQuery.of(MongoCriteria.where("_id").is(newId));
	}

	@Override
	public NoSQLQuery getQueryForVersion() {
		NoSQLDescriptionField descriptionIdField = descriptionEntity.getDescriptionIdField();
		NoSQLDescriptionField descriptionVersionField = descriptionEntity.getDescriptionVersionField();

		return MongoQuery
				.of(MongoCriteria.where(descriptionIdField.getName()).is(descriptionIdField.getObjectValue(entity))//
						.and(descriptionVersionField.getName()).is(descriptionVersionField.getObjectValue(entity)));
	}

	@Override
	public NoSQLMappedDocument toMappedDocument() {
		final LinkedHashMap<Object, Object> involvedObjects = new LinkedHashMap<Object, Object>();
		final Document dbDoc = ((MongoObjectMapper) mapper).toDocument(ProxyHelper.unwrap(entity), involvedObjects);
		return MongoMappedDocument.of(dbDoc);
	}

	@Override
	public Object getVersion() {
		Number version = (Number) descriptionEntity.getVersionValue(entity);
		return version;
	}

	@Override
	public T getEntity() {
		return entity;
	}

	public NoSQLDescriptionEntity getDescriptionEntity() {
		return descriptionEntity;
	}

	@Override
	public T populateIdIfNecessary(Object id) {
		if (id == null) {
			return null;
		}

		T bean = getEntity();
		NoSQLDescriptionField idProperty = descriptionEntity.getDescriptionIdField();

		if (idProperty == null) {
			return bean;
		}

		if (idProperty.getObjectValue(bean) != null) {
			return bean;
		}

		Object newId = id;
		if (!idProperty.getField().getType().equals(ObjectId.class)) {
			if (id instanceof ObjectId) {
				newId = ((ObjectId) id).toString();
			}
		}

		idProperty.setObjectValue(bean, newId);

		return bean;
	}

	@Override
	public T initializeVersionProperty() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public T incrementVersion() {
		Number version = (Number) getVersion();
		version = version == null ? 0 : version.longValue() + 1;
		NoSQLDescriptionField descriptionVersionField = descriptionEntity.getDescriptionVersionField();
		descriptionVersionField.setObjectValue(entity, version);
		return entity;
	}

}
