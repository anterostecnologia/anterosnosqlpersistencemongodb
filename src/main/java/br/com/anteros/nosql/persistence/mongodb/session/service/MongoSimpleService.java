package br.com.anteros.nosql.persistence.mongodb.session.service;

import br.com.anteros.nosql.persistence.mongodb.query.MongoCriteria;
import br.com.anteros.nosql.persistence.mongodb.query.MongoQuery;
import br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor.ComparisonToCriteriaConverter;
import br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor.RsqlMongoAdapter;
import br.com.anteros.nosql.persistence.mongodb.session.repository.MongoSimpleRepository;
import br.com.anteros.nosql.persistence.session.NoSQLSessionFactory;
import br.com.anteros.nosql.persistence.session.query.NoSQLQuery;
import br.com.anteros.nosql.persistence.session.query.Page;
import br.com.anteros.nosql.persistence.session.query.Pageable;
import br.com.anteros.nosql.persistence.session.repository.NoSQLRepository;
import br.com.anteros.nosql.persistence.session.service.AbstractSimpleService;

public class MongoSimpleService<T, ID> extends AbstractSimpleService<T, ID> {
	private Class entityClass;

	public MongoSimpleService(NoSQLSessionFactory sessionFactory) {
		super(sessionFactory);
	}

	public MongoSimpleService() {

	}

	@Override
	protected NoSQLRepository<T, ID> doGetDefaultRepository(NoSQLSessionFactory sessionFactory, Class entityClass) {
		this.entityClass = entityClass;
		return new MongoSimpleRepository<T, ID>(sessionFactory, entityClass);
	}

	@Override
	public NoSQLQuery<?> parseRsql(String rsql) {
		RsqlMongoAdapter adapter = new RsqlMongoAdapter(new ComparisonToCriteriaConverter());
		MongoCriteria criteria = (MongoCriteria) adapter.getCriteria(rsql, entityClass);
		return MongoQuery.of(criteria);
	}


	

}
