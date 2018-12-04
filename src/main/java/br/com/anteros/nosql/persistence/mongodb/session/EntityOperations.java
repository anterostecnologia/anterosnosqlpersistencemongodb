package br.com.anteros.nosql.persistence.mongodb.session;

import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;

import br.com.anteros.core.utils.Assert;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionEntity;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionEntityManager;
import br.com.anteros.nosql.persistence.metadata.exception.NoSQLDescriptionEntityException;
import br.com.anteros.nosql.persistence.session.NoSQLEntityAdapter;
import br.com.anteros.nosql.persistence.session.mapping.AbstractNoSQLObjectMapper;
import br.com.anteros.nosql.persistence.session.query.NoSQLQuery;

public class EntityOperations {

	private static final String ID_FIELD = "_id";

	private NoSQLDescriptionEntityManager descriptionEntityManager;
	private AbstractNoSQLObjectMapper mapper;

	public EntityOperations(NoSQLDescriptionEntityManager descriptionEntityManager, AbstractNoSQLObjectMapper mapper) {
		this.descriptionEntityManager = descriptionEntityManager;
		this.mapper = mapper;
	}

	public <T> NoSQLEntityAdapter<T> forEntity(T entity) {

		Assert.notNull(entity, "Bean must not be null!");

		NoSQLDescriptionEntity descriptionEntity = descriptionEntityManager.getDescriptionEntity(entity.getClass());

		return MongoEntityAdapter.of(entity, descriptionEntity, mapper);
	}

	public String determineCollectionName(Class<?> entityClass) {
		NoSQLDescriptionEntity descriptionEntity = descriptionEntityManager.getDescriptionEntity(entityClass);
		if (descriptionEntity==null) {
			throw new NoSQLDescriptionEntityException("Classe "+entityClass.getName()+" não encontrada na lista de entidades gerenciadas.");
		}
		return descriptionEntity.getCollectionName();
	}

	public String getIdPropertyName(Class<?> entityClass) {

		Assert.notNull(entityClass, "Type must not be null!");

		NoSQLDescriptionEntity descriptionEntity = descriptionEntityManager.getDescriptionEntity(entityClass);

		if (descriptionEntity != null && descriptionEntity.getDescriptionIdField() != null) {
			return descriptionEntity.getDescriptionIdField().getName();
		}

		return ID_FIELD;
	}

	public NoSQLQuery<Document> getByIdInQuery(List<?> result) {
		// TODO Auto-generated method stub
		return null;
	}

	public String determineEntityCollectionName(Object obj) {
		return null == obj ? null : determineCollectionName(obj.getClass());
	}

	public Object convertIdProperty(Class<?> entityClass, Object id) {
		NoSQLDescriptionEntity descriptionEntity = descriptionEntityManager.getDescriptionEntity(entityClass);

		if (descriptionEntity != null && descriptionEntity.getDescriptionIdField() != null) {
			if (descriptionEntity.getDescriptionIdField().getRealType().equals(String.class)){
				return new ObjectId((String)id);
			}
		}
		return id;
	}

}
