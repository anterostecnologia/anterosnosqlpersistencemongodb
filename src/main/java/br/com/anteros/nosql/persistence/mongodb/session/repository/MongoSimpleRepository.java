package br.com.anteros.nosql.persistence.mongodb.session.repository;

import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.bson.Document;

import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionEntity;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;
import br.com.anteros.nosql.persistence.mongodb.query.MongoCriteria;
import br.com.anteros.nosql.persistence.mongodb.query.MongoQuery;
import br.com.anteros.nosql.persistence.session.NoSQLSession;
import br.com.anteros.nosql.persistence.session.NoSQLSessionFactory;
import br.com.anteros.nosql.persistence.session.query.Example;
import br.com.anteros.nosql.persistence.session.query.NoSQLQuery;
import br.com.anteros.nosql.persistence.session.query.Page;
import br.com.anteros.nosql.persistence.session.query.Pageable;
import br.com.anteros.nosql.persistence.session.query.Sort;
import br.com.anteros.nosql.persistence.session.repository.AbstractSimpleRepository;

@SuppressWarnings("rawtypes")
public class MongoSimpleRepository<T, ID> extends AbstractSimpleRepository<T, ID> {

	public MongoSimpleRepository(NoSQLSession session, Class<T> type) {
		super(session, type);
	}

	public MongoSimpleRepository(NoSQLSession session) {
		super(session);
	}

	public MongoSimpleRepository(NoSQLSessionFactory sessionFactory, Class<T> type) {
		super(sessionFactory, type);
	}

	public MongoSimpleRepository(NoSQLSessionFactory sessionFactory) {
		super(sessionFactory);
	}

	@Override
	protected NoSQLQuery doGetIdQuery(Object id) {
		NoSQLDescriptionEntity descriptionEntity = getSession().getDescriptionEntityManager().getDescriptionEntity(persistentClass);
		NoSQLDescriptionField descriptionIdField = descriptionEntity.getDescriptionIdField();
		return MongoQuery.of(MongoCriteria.where(descriptionIdField.getName()).is(id));
	}

	@Override
	protected NoSQLQuery doGetQueryWithSort(Sort sort) {
		return MongoQuery.of().with(sort);
	}

	@Override
	protected NoSQLQuery doGetEmptyQuery() {
		return MongoQuery.of();
	}

	@Override
	protected NoSQLQuery doGetQueryWithPage(Pageable page) {
		return MongoQuery.of().with(page);
	}

	@Override
	protected NoSQLQuery doGetQueryByExample(Example<?> example, Sort sort, Pageable page) {
		return MongoQuery.of(MongoCriteria.byExample(example)).with(sort).with(page);
	}

	@Override
	protected NoSQLQuery doGetIdsQuery(Iterable<ID> ids) {
		NoSQLDescriptionEntity descriptionEntity = getSession().getDescriptionEntityManager()
				.getDescriptionEntity(persistentClass);
		NoSQLDescriptionField descriptionIdField = descriptionEntity.getDescriptionIdField();
		Stream<ID> stream = StreamSupport.stream(ids.spliterator(), false);
		return MongoQuery.of(MongoCriteria.where(descriptionIdField.getName()).in(stream.collect(Collectors.toList())));
	}

	
	@Override
	protected NoSQLQuery doParseQuery(String query) {
		return MongoQuery.of(Document.parse(query));
	}

	

}
