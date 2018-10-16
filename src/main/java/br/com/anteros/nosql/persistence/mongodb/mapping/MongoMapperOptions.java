package br.com.anteros.nosql.persistence.mongodb.mapping;

import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionEntityManager;
import br.com.anteros.nosql.persistence.session.cache.DefaultNoSQLEntityCacheFactory;
import br.com.anteros.nosql.persistence.session.cache.NoSQLEntityCacheFactory;
import br.com.anteros.nosql.persistence.session.mapping.NoSQLCustomMapper;
import br.com.anteros.nosql.persistence.session.mapping.NoSQLMapperOptions;
import br.com.anteros.nosql.persistence.session.mapping.NoSQLObjectFactory;

public class MongoMapperOptions extends NoSQLMapperOptions {
	
	protected NoSQLCustomMapper defaultMapper = new MongoEmbeddedMapper();
	protected NoSQLCustomMapper embeddedMapper = new MongoEmbeddedMapper();
	protected NoSQLObjectFactory defaultCreator = new DefaultMongoCreator(this);
	protected NoSQLCustomMapper referenceMapper = new MongoReferenceMapper();
	protected NoSQLCustomMapper valueMapper = new MongoValueMapper();
	protected NoSQLEntityCacheFactory cacheFactory = new DefaultNoSQLEntityCacheFactory();
	protected boolean cacheClassLookups;
	protected boolean ignoreFinals;
	protected boolean storeEmpties;
	protected boolean storeNulls;
	protected boolean useLowerCaseCollectionNames;

	public MongoMapperOptions(NoSQLDescriptionEntityManager descriptionEntityManager) {
		super(descriptionEntityManager);
	}

	@Override
	public NoSQLCustomMapper getDefaultMapper() {
		return defaultMapper;
	}

	@Override
	public NoSQLCustomMapper getEmbeddedMapper() {
		return embeddedMapper;
	}

	@Override
	public NoSQLObjectFactory getObjectFactory() {
		return defaultCreator;
	}

	@Override
	public NoSQLCustomMapper getReferenceMapper() {
		return referenceMapper;
	}

	@Override
	public NoSQLCustomMapper getValueMapper() {
		return valueMapper;
	}

	@Override
	public boolean isCacheClassLookups() {
		return cacheClassLookups;
	}

	@Override
	public boolean isIgnoreFinals() {
		return ignoreFinals;
	}

	@Override
	public boolean isStoreEmpties() {
		return storeEmpties;
	}

	@Override
	public boolean isStoreNulls() {
		return storeNulls;
	}

	@Override
	public boolean isUseLowerCaseCollectionNames() {
		return useLowerCaseCollectionNames;
	}

	@Override
	public NoSQLEntityCacheFactory getCacheFactory() {
		return cacheFactory;
	}

	public NoSQLObjectFactory getDefaultCreator() {
		return defaultCreator;
	}

	public NoSQLMapperOptions defaultCreator(NoSQLObjectFactory defaultCreator) {
		this.defaultCreator = defaultCreator;
		return this;
	}

	public NoSQLMapperOptions defaultMapper(NoSQLCustomMapper defaultMapper) {
		this.defaultMapper = defaultMapper;
		return this;
	}

	public NoSQLMapperOptions embeddedMapper(NoSQLCustomMapper embeddedMapper) {
		this.embeddedMapper = embeddedMapper;
		return this;
	}

	public NoSQLMapperOptions referenceMapper(NoSQLCustomMapper referenceMapper) {
		this.referenceMapper = referenceMapper;
		return this;
	}

	public NoSQLMapperOptions valueMapper(NoSQLCustomMapper valueMapper) {
		this.valueMapper = valueMapper;
		return this;
	}

	public NoSQLMapperOptions cacheFactory(NoSQLEntityCacheFactory cacheFactory) {
		this.cacheFactory = cacheFactory;
		return this;
	}

	public NoSQLMapperOptions cacheClassLookups(boolean cacheClassLookups) {
		this.cacheClassLookups = cacheClassLookups;
		return this;
	}

	public NoSQLMapperOptions ignoreFinals(boolean ignoreFinals) {
		this.ignoreFinals = ignoreFinals;
		return this;
	}

	public NoSQLMapperOptions storeEmpties(boolean storeEmpties) {
		this.storeEmpties = storeEmpties;
		return this;
	}

	public NoSQLMapperOptions storeNulls(boolean storeNulls) {
		this.storeNulls = storeNulls;
		return this;
	}

	public NoSQLMapperOptions useLowerCaseCollectionNames(boolean useLowerCaseCollectionNames) {
		this.useLowerCaseCollectionNames = useLowerCaseCollectionNames;
		return this;
	}

}
