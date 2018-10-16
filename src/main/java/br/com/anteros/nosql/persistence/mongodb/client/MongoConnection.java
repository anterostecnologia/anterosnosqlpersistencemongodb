package br.com.anteros.nosql.persistence.mongodb.client;

import org.springframework.aop.framework.ProxyFactory;

import com.mongodb.ClientSessionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import br.com.anteros.core.utils.Assert;
import br.com.anteros.nosql.persistence.client.NoSQLConnection;
import br.com.anteros.nosql.persistence.mongodb.session.SessionAwareMethodInterceptor;
import br.com.anteros.nosql.persistence.mongodb.transaction.MongoTransaction;
import br.com.anteros.nosql.persistence.session.exception.NoSQLDataAccessException;
import br.com.anteros.nosql.persistence.session.exception.NoSQLExceptionTranslator;
import br.com.anteros.nosql.persistence.session.transaction.NoSQLTransaction;

public class MongoConnection implements NoSQLConnection {

	private final MongoClient mongoClient;
	private final String databaseName;
	private final boolean mongoInstanceCreated;
	private final NoSQLExceptionTranslator exceptionTranslator;
	private ClientSession clientSession;

	private WriteConcern writeConcern;

	public MongoConnection(MongoClient mongoClient, String databaseName, boolean mongoInstanceCreated,
			NoSQLExceptionTranslator exceptionTranslator) {

		Assert.notNull(mongoClient, "MongoClient must not be null!");
		Assert.hasText(databaseName, "Database name must not be empty!");
		Assert.isTrue(databaseName.matches("[^/\\\\.$\"\\s]+"),
				"Database name must not contain slashes, dots, spaces, quotes, or dollar signs!");

		this.mongoClient = mongoClient;
		this.databaseName = databaseName;
		this.mongoInstanceCreated = mongoInstanceCreated;
		this.exceptionTranslator = exceptionTranslator;
	}

	@Override
	public boolean isClosed() {
		return false;
	}

	@Override
	public void close() {
		try {
			destroy();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void setWriteConcern(WriteConcern writeConcern) {
		this.writeConcern = writeConcern;
	}

	public MongoDatabase getDatabase() throws NoSQLDataAccessException {
		return getDatabase(databaseName);
	}

	public MongoDatabase getDatabase(String dbName) throws NoSQLDataAccessException {
		Assert.hasText(dbName, "Database name must not be empty!");
		MongoDatabase db = doGetMongoDatabase(dbName);
		if (writeConcern == null) {
			return db;
		}
		return db.withWriteConcern(writeConcern);
	}

	public MongoDatabase getDatabase(NoSQLTransaction transaction) throws NoSQLDataAccessException {
		return getDatabase(databaseName);
	}

	public MongoDatabase getDatabase(String dbName, NoSQLTransaction transaction) throws NoSQLDataAccessException {
		Assert.hasText(dbName, "Database name must not be empty!");
		MongoDatabase db = doGetMongoDatabase(dbName);
		if (writeConcern == null) {
			return db;
		}
		return db.withWriteConcern(writeConcern);
	}

	protected MongoDatabase doGetMongoDatabase(String dbName) {
		return getMongoClient().getDatabase(dbName);
	}

	protected MongoDatabase doGetMongoDatabase(String dbName, NoSQLTransaction transaction) {
		return proxyDatabase(((MongoTransaction) transaction).getClientSession(), getMongoClient().getDatabase(dbName));
	}

	public ClientSession getSession(ClientSessionOptions options) {
		if (clientSession == null
				|| (clientSession.getServerSession() == null || clientSession.getServerSession().isClosed())) {
			clientSession = getMongoClient().startSession(options);
		}
		return clientSession;
	}

	public void destroy() throws Exception {
		if (mongoInstanceCreated) {
			closeClient();
		}
	}

	protected void closeClient() {
		//getMongoClient().close();
	}

	public NoSQLExceptionTranslator getExceptionTranslator() {
		return this.exceptionTranslator;
	}

	public NoSQLConnection withSession(ClientSession session) {
		return new MongoConnection.ClientSessionBoundMongoConnection(session, this);
	}

	public MongoClient getMongoClient() {
		return mongoClient;
	}

	protected String getDefaultDatabaseName() {
		return databaseName;
	}

	private MongoDatabase proxyDatabase(ClientSession session, MongoDatabase database) {
		return createProxyInstance(session, database, MongoDatabase.class);
	}

	private MongoCollection<?> proxyCollection(ClientSession session, MongoCollection<?> collection) {
		return createProxyInstance(session, collection, MongoCollection.class);
	}

	private <T> T createProxyInstance(ClientSession session, T target, Class<T> targetType) {

		ProxyFactory factory = new ProxyFactory();
		factory.setTarget(target);
		factory.setInterfaces(targetType);
		factory.setOpaque(true);
		factory.addAdvice(new SessionAwareMethodInterceptor<>(session, target, ClientSession.class, MongoDatabase.class,
				this::proxyDatabase, MongoCollection.class, this::proxyCollection));

		return targetType.cast(factory.getProxy());
	}

	static class ClientSessionBoundMongoConnection implements NoSQLConnection {

		private ClientSession session;
		private MongoConnection delegate;

		public ClientSessionBoundMongoConnection(ClientSession session, MongoConnection delegate) {
			this.session = session;
			this.delegate = delegate;
		}

		public MongoDatabase getDatabase() throws NoSQLDataAccessException {
			return proxyMongoDatabase(delegate.getDatabase());
		}

		public MongoDatabase getDatabase(String dbName) throws NoSQLDataAccessException {
			return proxyMongoDatabase(delegate.getDatabase(dbName));
		}

		public NoSQLExceptionTranslator getExceptionTranslator() {
			return delegate.getExceptionTranslator();
		}

		public ClientSession getSession(ClientSessionOptions options) {
			return delegate.getSession(options);
		}

		public NoSQLConnection withSession(ClientSession session) {
			return delegate.withSession(session);
		}

		private MongoDatabase proxyMongoDatabase(MongoDatabase database) {
			return createProxyInstance(session, database, MongoDatabase.class);
		}

		private MongoDatabase proxyDatabase(ClientSession session, MongoDatabase database) {
			return createProxyInstance(session, database, MongoDatabase.class);
		}

		private MongoCollection<?> proxyCollection(ClientSession session, MongoCollection<?> collection) {
			return createProxyInstance(session, collection, MongoCollection.class);
		}

		private <T> T createProxyInstance(ClientSession session, T target, Class<T> targetType) {

			ProxyFactory factory = new ProxyFactory();
			factory.setTarget(target);
			factory.setInterfaces(targetType);
			factory.setOpaque(true);
			factory.addAdvice(new SessionAwareMethodInterceptor<>(session, target, ClientSession.class,
					MongoDatabase.class, this::proxyDatabase, MongoCollection.class, this::proxyCollection));

			return targetType.cast(factory.getProxy());
		}

		@Override
		public boolean isClosed() {
			return delegate.isClosed();
		}

		@Override
		public void close() {
			delegate.close();
		}
	}

}
