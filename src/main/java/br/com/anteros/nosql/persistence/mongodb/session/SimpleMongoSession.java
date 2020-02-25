package br.com.anteros.nosql.persistence.mongodb.session;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.bson.Document;

import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;

import br.com.anteros.core.log.Logger;
import br.com.anteros.core.log.LoggerProvider;
import br.com.anteros.core.utils.Assert;
import br.com.anteros.core.utils.CollectionUtils;
import br.com.anteros.core.utils.ObjectUtils;
import br.com.anteros.core.utils.ResourceUtils;
import br.com.anteros.core.utils.StringUtils;
import br.com.anteros.nosql.persistence.client.NoSQLConnection;
import br.com.anteros.nosql.persistence.converters.Key;
import br.com.anteros.nosql.persistence.dialect.NoSQLDialect;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionEntity;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionEntityException;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionEntityManager;
import br.com.anteros.nosql.persistence.mongodb.aggregation.AggregationPipeline;
import br.com.anteros.nosql.persistence.mongodb.aggregation.AggregationPipelineImpl;
import br.com.anteros.nosql.persistence.mongodb.client.MongoConnection;
import br.com.anteros.nosql.persistence.mongodb.mapping.MongoObjectMapper;
import br.com.anteros.nosql.persistence.mongodb.query.InvalidDataAccessApiUsageException;
import br.com.anteros.nosql.persistence.mongodb.query.MongoQuery;
import br.com.anteros.nosql.persistence.mongodb.query.MongoUpdate;
import br.com.anteros.nosql.persistence.mongodb.query.SerializationUtils;
import br.com.anteros.nosql.persistence.mongodb.session.event.MongoAfterConvertEvent;
import br.com.anteros.nosql.persistence.mongodb.session.event.MongoAfterDeleteEvent;
import br.com.anteros.nosql.persistence.mongodb.session.event.MongoAfterLoadEvent;
import br.com.anteros.nosql.persistence.mongodb.session.event.MongoAfterSaveEvent;
import br.com.anteros.nosql.persistence.mongodb.session.event.MongoBeforeConvertEvent;
import br.com.anteros.nosql.persistence.mongodb.session.event.MongoBeforeDeleteEvent;
import br.com.anteros.nosql.persistence.mongodb.session.event.MongoBeforeSaveEvent;
import br.com.anteros.nosql.persistence.mongodb.session.event.MongoEventPublisher;
import br.com.anteros.nosql.persistence.proxy.ProxyHelper;
import br.com.anteros.nosql.persistence.session.FindAndModifyOptions;
import br.com.anteros.nosql.persistence.session.FindAndReplaceOptions;
import br.com.anteros.nosql.persistence.session.NoSQLEntityAdapter;
import br.com.anteros.nosql.persistence.session.NoSQLMappedDocument;
import br.com.anteros.nosql.persistence.session.NoSQLPersistenceContext;
import br.com.anteros.nosql.persistence.session.NoSQLResult;
import br.com.anteros.nosql.persistence.session.NoSQLSession;
import br.com.anteros.nosql.persistence.session.NoSQLSessionException;
import br.com.anteros.nosql.persistence.session.NoSQLSessionFactory;
import br.com.anteros.nosql.persistence.session.NoSQLSessionListener;
import br.com.anteros.nosql.persistence.session.ShowCommandsType;
import br.com.anteros.nosql.persistence.session.cache.DefaultNoSQLEntityCache;
import br.com.anteros.nosql.persistence.session.cache.NoSQLEntityCache;
import br.com.anteros.nosql.persistence.session.command.CommandNoSQL;
import br.com.anteros.nosql.persistence.session.event.NoSQLEvent;
import br.com.anteros.nosql.persistence.session.event.NoSQLEventPublisher;
import br.com.anteros.nosql.persistence.session.exception.NoSQLDataAccessException;
import br.com.anteros.nosql.persistence.session.exception.NoSQLExceptionTranslator;
import br.com.anteros.nosql.persistence.session.impl.SimpleNoSQLPersistenceContext;
import br.com.anteros.nosql.persistence.session.mapping.AbstractNoSQLObjectMapper;
import br.com.anteros.nosql.persistence.session.query.NoSQLCriteria;
import br.com.anteros.nosql.persistence.session.query.NoSQLQuery;
import br.com.anteros.nosql.persistence.session.query.NoSQLUpdate;
import br.com.anteros.nosql.persistence.session.transaction.NoSQLTransaction;
import br.com.anteros.nosql.persistence.session.transaction.NoSQLTransactionException;
import br.com.anteros.nosql.persistence.session.transaction.NoSQLTransactionFactory;

public class SimpleMongoSession implements MongoSession {

	private static Logger LOG = LoggerProvider.getInstance().getLogger(NoSQLSession.class);

	private static final Collection<String> ITERABLE_CLASSES;

	static {

		Set<String> iterableClasses = new HashSet<>();
		iterableClasses.add(List.class.getName());
		iterableClasses.add(Collection.class.getName());
		iterableClasses.add(Iterator.class.getName());

		ITERABLE_CLASSES = Collections.unmodifiableCollection(iterableClasses);
	}

	private NoSQLConnection connection;
	private List<NoSQLSessionListener> listeners = new ArrayList<NoSQLSessionListener>();
	private List<CommandNoSQL> commandQueue = new ArrayList<CommandNoSQL>();
	private NoSQLSessionFactory sessionFactory;
	private NoSQLDescriptionEntityManager descriptionEntityManager;
	private NoSQLTransactionFactory transactionFactory;
	private boolean formatCommands;
	private NoSQLDialect dialect;
	private ShowCommandsType[] showCommands;
	private NoSQLPersistenceContext persistenceContext;
	private NoSQLTransaction transaction;
	private boolean validationActive;
	private MongoObjectMapper mapper;
	private NoSQLEntityCache cache;
	private WriteConcern defaultWriteConcern;
	private EntityOperations operations;
	private NoSQLExceptionTranslator exceptionTranslator;
	private ReadPreference readPreference;
	private NoSQLEventPublisher<Document> eventPublisher = new MongoEventPublisher();
	private boolean withoutTransactionControl;

	public SimpleMongoSession(NoSQLSessionFactory sessionFactory, NoSQLConnection connection,
			NoSQLDescriptionEntityManager descriptionEntityManager, NoSQLDialect dialect,
			ShowCommandsType[] showCommands, boolean formatCommands, NoSQLTransactionFactory transactionFactory,
			boolean validationActive, boolean withoutTransactionControl) {

		this.sessionFactory = sessionFactory;
		this.connection = connection;
		this.descriptionEntityManager = descriptionEntityManager;
		this.dialect = dialect;
		this.showCommands = showCommands;
		this.persistenceContext = new SimpleNoSQLPersistenceContext(this, descriptionEntityManager);
		this.validationActive = validationActive;
		this.mapper = new MongoObjectMapper(descriptionEntityManager);
		this.cache = new DefaultNoSQLEntityCache();
//		this.defaultWriteConcern = ((MongoConnection) connection).getWriteConcern(); // AQUI
		this.operations = new EntityOperations(descriptionEntityManager, mapper);
		this.exceptionTranslator = new MongoExceptionTranslator();
		this.transactionFactory = transactionFactory;
		this.withoutTransactionControl = withoutTransactionControl;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#flush()
	 */
	@Override
	public void flush() throws Exception {
		errorIfClosed();
		for (CommandNoSQL command : commandQueue) {
			command.execute();
		}
		commandQueue.clear();
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#forceFlush(java.util.Set)
	 */
	@Override
	public void forceFlush(Set<String> collectionNames) throws Exception {
		errorIfClosed();
		if (collectionNames != null) {
			synchronized (commandQueue) {
				boolean foundCommand = false;
				for (CommandNoSQL command : commandQueue) {
					if (collectionNames.contains(command.getTargetCollectionName().toUpperCase())) {
						foundCommand = true;
						break;
					}
				}
				if (foundCommand) {
					flush();
				}
			}
		}

	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#onBeforeExecuteCommit(br.com.anteros.nosql.persistence.client.NoSQLConnection)
	 */
	@Override
	public void onBeforeExecuteCommit(NoSQLConnection connection) throws Exception {
		flush();
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#onBeforeExecuteRollback(br.com.anteros.nosql.persistence.client.NoSQLConnection)
	 */
	@Override
	public void onBeforeExecuteRollback(NoSQLConnection connection) throws Exception {
		if (this.getConnection() == connection) {
			synchronized (commandQueue) {
				commandQueue.clear();
			}
		}
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#onAfterExecuteCommit(br.com.anteros.nosql.persistence.client.NoSQLConnection)
	 */
	@Override
	public void onAfterExecuteCommit(NoSQLConnection connection) throws Exception {

	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#onAfterExecuteRollback(br.com.anteros.nosql.persistence.client.NoSQLConnection)
	 */
	@Override
	public void onAfterExecuteRollback(NoSQLConnection connection) throws Exception {

	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#getDescriptionEntityManager()
	 */
	@Override
	public NoSQLDescriptionEntityManager getDescriptionEntityManager() {
		return descriptionEntityManager;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#getDialect()
	 */
	@Override
	public NoSQLDialect getDialect() {
		return dialect;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#getConnection()
	 */
	@Override
	public NoSQLConnection getConnection() {
		return connection;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#getPersistenceContext()
	 */
	@Override
	public NoSQLPersistenceContext getPersistenceContext() {
		return persistenceContext;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#addListener(br.com.anteros.nosql.persistence.session.NoSQLSessionListener)
	 */
	@Override
	public void addListener(NoSQLSessionListener listener) {
		if (!listeners.contains(listener))
			listeners.add(listener);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#removeListener(br.com.anteros.nosql.persistence.session.NoSQLSessionListener)
	 */
	@Override
	public void removeListener(NoSQLSessionListener listener) {
		if (listeners.contains(listener))
			listeners.remove(listener);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#getListeners()
	 */
	@Override
	public List<NoSQLSessionListener> getListeners() {
		return listeners;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#setFormatCommands(boolean)
	 */
	@Override
	public void setFormatCommands(boolean format) {
		this.formatCommands = format;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#isShowCommands()
	 */
	@Override
	public boolean isShowCommands() {
		return showCommands != null;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#isFormatCommands()
	 */
	@Override
	public boolean isFormatCommands() {
		return formatCommands;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#getDatabase()
	 */
	@Override
	public MongoDatabase getDatabase() {
		MongoConnection mongoConnection = (MongoConnection) getConnection();
		if (transaction != null && transaction.isActive())
			return mongoConnection.getDatabase(transaction);
		else
			return mongoConnection.getDatabase();
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#getDatabase(java.lang.String)
	 */
	@Override
	public MongoDatabase getDatabase(String dbName) {
		MongoConnection mongoConnection = (MongoConnection) getConnection();
		if (transaction != null && transaction.isActive())
			return mongoConnection.getDatabase(dbName, transaction);
		else
			return mongoConnection.getDatabase(dbName);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#dropCollection(java.lang.String)
	 */
	@Override
	public void dropCollection(String collectionName) {
		Assert.notNull(collectionName, "CollectionName must not be null!");

		execute(collectionName, new CollectionCallback<Void>() {
			public Void doInCollection(MongoCollection<Document> collection)
					throws MongoException, NoSQLDataAccessException {
				collection.drop();
				if (LOG.isDebugEnabled()) {
					LOG.debug("Dropped collection [{}]",
							collection.getNamespace() != null ? collection.getNamespace().getCollectionName()
									: collectionName);
				}
				return null;
			}
		});
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#isProxyObject(java.lang.Object)
	 */
	@Override
	public boolean isProxyObject(Object object) throws Exception {
		return ProxyHelper.isProxy(object);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#proxyIsInitialized(java.lang.Object)
	 */
	@Override
	public boolean proxyIsInitialized(Object object) throws Exception {
		return ProxyHelper.isUnFetched(object);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#cloneEntityManaged(java.lang.Object)
	 */
	@Override
	public <T> T cloneEntityManaged(Object object) throws Exception {
		return null;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#evict(java.lang.Class)
	 */
	@Override
	public void evict(Class<?> object) {
		errorIfClosed();
		persistenceContext.evict(object);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#evictAll()
	 */
	@Override
	public void evictAll() {
		errorIfClosed();
		persistenceContext.evictAll();
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#isClosed()
	 */
	@Override
	public boolean isClosed() throws Exception {
		return getConnection() == null || getConnection().isClosed();
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#getTransaction()
	 */
	@Override
	public NoSQLTransaction getTransaction() {
		if (transaction == null || !transaction.isActive()) {
			transaction = transactionFactory.createTransaction(getConnection(), getPersistenceContext());
		}
		return transaction;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#getNoSQLSessionFactory()
	 */
	@Override
	public NoSQLSessionFactory getNoSQLSessionFactory() {
		return sessionFactory;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#clear()
	 */
	@Override
	public void clear() throws Exception {
		internalClear();
	}

	private void internalClear() {
		persistenceContext.evictAll();
		persistenceContext.clearCache();
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#validationIsActive()
	 */
	@Override
	public boolean validationIsActive() {
		return validationActive;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#activateValidation()
	 */
	@Override
	public void activateValidation() {
		this.validationActive = true;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#deactivateValidation()
	 */
	@Override
	public void deactivateValidation() {
		this.validationActive = false;
	}

	@Override
	protected void finalize() throws Throwable {
		if (!this.isClosed())
			this.close();
		super.finalize();
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#close()
	 */
	@Override
	public void close() throws Exception {
		for (NoSQLSessionListener listener : listeners)
			listener.onClose(this);
		synchronized (commandQueue) {
			commandQueue.clear();
		}
		if (connection != null && !connection.isClosed())
			connection.close();
		connection = null;
		LOG.debug("Fechou session " + this);
	}

	protected void errorIfClosed() {
		try {
			if (isClosed()) {
				throw new NoSQLSessionException("Sessão está fechada!");
			}
		} catch (Exception ex) {
			throw new NoSQLSessionException("Sessão está fechada!", ex);
		}
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#getIdentifier(T)
	 */
	@Override
	public <T> Object getIdentifier(T owner) throws Exception {
		NoSQLDescriptionEntity descriptionEntity = descriptionEntityManager.getDescriptionEntity(owner.getClass());
		if (descriptionEntity == null)
			return null;

		return descriptionEntity.getIdentifierValue(owner);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#collectionExists(java.lang.Class)
	 */
	@Override
	public <T> boolean collectionExists(Class<T> entityClass) {
		NoSQLDescriptionEntity descriptionEntity = getDescriptionEntity(entityClass);
		String collectionName = descriptionEntity.getCollectionName();
		return this.collectionExists(collectionName);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#collectionExists(java.lang.String)
	 */
	@Override
	public boolean collectionExists(String collectionName) {

		Assert.notNull(collectionName, "CollectionName must not be null!");

		return execute(new DbCallback<Boolean>() {
			public Boolean doInDB(MongoDatabase db) throws MongoException, NoSQLDataAccessException {

				for (String name : db.listCollectionNames()) {
					if (name.equals(collectionName)) {
						return true;
					}
				}
				return false;
			}
		});
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#createAggregation(java.lang.Class)
	 */
	@Override
	public AggregationPipeline createAggregation(final Class<?> source) {
		return new AggregationPipelineImpl(this, mapper, getCollection(source), source);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#createAggregation(java.lang.String, java.lang.Class)
	 */
	@Override
	public AggregationPipeline createAggregation(final String collection, final Class<?> clazz) {
		return new AggregationPipelineImpl(this, mapper, getCollection(collection), clazz);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#count(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.Class)
	 */
	@Override
	public <T> long count(NoSQLQuery<Document> query, Class<?> entityClass) {
		Assert.notNull(entityClass, "Entity class must not be null!");
		return count(query, entityClass, operations.determineCollectionName(entityClass));

	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#count(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> long count(NoSQLQuery<Document> query, Class<?> entityClass, String collectionName) {
		return count(query, null, collectionName);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#count(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.String)
	 */
	@Override
	public <T> long count(NoSQLQuery<Document> query, String collectionName) {
		Assert.notNull(query, "Query must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		CountOptions options = new CountOptions();

		Document document = query.getQueryObject();

		return execute(collectionName, collection -> collection.count(document, options));
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#createCollection(java.lang.Class)
	 */
	@Override
	public <T> MongoCollection<Document> createCollection(Class<T> entityClass) throws Exception {
		NoSQLDescriptionEntity descriptionEntity = getDescriptionEntity(entityClass);
		String collectionName = descriptionEntity.getCollectionName();
		return this.createCollection(collectionName);
	}

	protected <T> NoSQLDescriptionEntity getDescriptionEntity(Class<T> entityClass) {
		NoSQLDescriptionEntity descriptionEntity = descriptionEntityManager.getDescriptionEntity(entityClass);
		if (descriptionEntity == null) {
			new NoSQLDescriptionEntityException(
					"Entidade não encontrada na lista de entidades gerenciadas. " + entityClass.getName());
		}
		return descriptionEntity;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#createCollection(java.lang.String)
	 */
	@Override
	public MongoCollection<Document> createCollection(String collectionName) throws Exception {
		return getDatabase().getCollection(collectionName);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#dropCollection(java.lang.Class)
	 */
	@Override
	public <T> void dropCollection(Class<T> entityClass) {
		dropCollection(operations.determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#execute(java.lang.Class, br.com.anteros.nosql.persistence.mongodb.session.CollectionCallback)
	 */
	@Override
	public <T> T execute(Class<?> entityClass, CollectionCallback<T> callback) throws Exception {
		Assert.notNull(entityClass, "EntityClass must not be null!");
		return execute(operations.determineCollectionName(entityClass), callback);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#execute(br.com.anteros.nosql.persistence.mongodb.session.DbCallback)
	 */
	@Override
	public <T> T execute(DbCallback<T> action) {
		Assert.notNull(action, "DbCallback must not be null!");

		try {
			MongoDatabase db = prepareDatabase(this.getDatabase());
			return action.doInDB(db);
		} catch (RuntimeException e) {
			throw translateRuntimeException(e, exceptionTranslator);
		}
	}

	protected MongoDatabase prepareDatabase(MongoDatabase database) {
		return database;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#execute(java.lang.String, br.com.anteros.nosql.persistence.mongodb.session.CollectionCallback)
	 */
	@Override
	public <T> T execute(String collectionName, CollectionCallback<T> callback) {
		Assert.notNull(collectionName, "CollectionName must not be null!");
		Assert.notNull(callback, "CollectionCallback must not be null!");

		try {
			MongoCollection<Document> collection = getAndPrepareCollection(getDatabase(), collectionName);
			return callback.doInCollection(collection);
		} catch (RuntimeException e) {
			throw translateRuntimeException(e, exceptionTranslator);
		}
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#executeCommand(org.bson.Document)
	 */
	@Override
	public Document executeCommand(Document command) throws Exception {
		return getDatabase().runCommand(command);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#executeCommand(org.bson.Document, com.mongodb.ReadPreference)
	 */
	@Override
	public Document executeCommand(Document command, ReadPreference readPreference) throws Exception {
		return getDatabase().runCommand(command, readPreference);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#executeCommand(java.lang.String)
	 */
	@Override
	public Document executeCommand(String jsonCommand) throws Exception {
		return getDatabase().runCommand(Document.parse(jsonCommand), Document.class);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#executeQuery(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.String, br.com.anteros.nosql.persistence.mongodb.session.DocumentCallbackHandler)
	 */
	@Override
	public void executeQuery(NoSQLQuery<Document> query, String collectionName, DocumentCallbackHandler dch)
			throws Exception {
		executeQuery(query, collectionName, dch, new QueryCursorPreparer(query, null));

	}

	protected void executeQuery(NoSQLQuery<Document> query, String collectionName,
			DocumentCallbackHandler documentCallbackHandler, CursorPreparer preparer) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");
		Assert.notNull(documentCallbackHandler, "DocumentCallbackHandler must not be null!");

		Document queryObject = query.getQueryObject();
		Document sortObject = query.getSortObject();
		Document fieldsObject = query.getFieldsObject();

		if (LOG.isDebugEnabled()) {
			LOG.debug("Executing query: {} sort: {} fields: {} in collection: {}",
					SerializationUtils.serializeToJsonSafely(queryObject), sortObject, fieldsObject, collectionName);
		}

		this.executeQueryInternal(new FindCallback(queryObject, fieldsObject), preparer, documentCallbackHandler,
				collectionName);
	}

	private void executeQueryInternal(CollectionCallback<FindIterable<Document>> collectionCallback,
			CursorPreparer preparer, DocumentCallbackHandler callbackHandler, String collectionName) {

		try {

			MongoCursor<Document> cursor = null;

			try {
				FindIterable<Document> iterable = collectionCallback
						.doInCollection(getAndPrepareCollection(getDatabase(), collectionName));

				if (preparer != null) {
					iterable = preparer.prepare(iterable);
				}

				cursor = iterable.iterator();

				while (cursor.hasNext()) {
					callbackHandler.processDocument(cursor.next());
				}
			} finally {
				if (cursor != null) {
					cursor.close();
				}
			}
		} catch (RuntimeException e) {
			throw translateRuntimeException(e, exceptionTranslator);
		}
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#exists(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.Class)
	 */
	@Override
	public <T> boolean exists(NoSQLQuery<Document> query, Class<?> entityClass) {
		return exists(query, entityClass, operations.determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#exists(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> boolean exists(NoSQLQuery<Document> query, Class<?> entityClass, String collectionName) {
		if (query == null) {
			throw new InvalidDataAccessApiUsageException("Query passed in to exist can't be null");
		}
		Assert.notNull(collectionName, "CollectionName must not be null!");

		Document mappedQuery = query.getQueryObject();

		return execute(collectionName, new ExistsCallback(mappedQuery));
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#exists(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.String)
	 */
	@Override
	public <T> boolean exists(NoSQLQuery<Document> query, String collectionName) {
		return this.count(query, collectionName) > 0;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#find(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.Class)
	 */
	@Override
	public <T> List<T> find(NoSQLQuery<Document> query, Class<T> entityClass) {
		return find(query, entityClass, operations.determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#find(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> List<T> find(NoSQLQuery<Document> query, Class<T> entityClass, String collectionName) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");
		Assert.notNull(entityClass, "EntityClass must not be null!");

		return doFind(collectionName, query.getQueryObject(), query.getFieldsObject(), entityClass,
				new QueryCursorPreparer(query, entityClass));
	}

	protected <T> List<T> doFind(String collectionName, Document query, Document fields, Class<T> entityClass,
			CursorPreparer preparer) {
		return doFind(collectionName, query, fields, entityClass, preparer,
				new ReadDocumentCallback<>(mapper, entityClass, collectionName));
	}

	protected <S, T> List<T> doFind(String collectionName, Document query, Document fields, Class<S> entityClass,
			CursorPreparer preparer, DocumentCallback<T> objectCallback) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("find using query: {} fields: {} for class: {} in collection: {}",
					SerializationUtils.serializeToJsonSafely(query), fields, entityClass, collectionName);
		}

		return executeFindMultiInternal(new FindCallback(query, fields), preparer, objectCallback, collectionName);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#findAll(java.lang.Class)
	 */
	@Override
	public <T> List<T> findAll(Class<T> entityClass) {
		return findAll(entityClass, operations.determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#findAll(java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> List<T> findAll(Class<T> entityClass, String collectionName) {
		return executeFindMultiInternal(new FindCallback(new Document(), new Document()), null,
				new ReadDocumentCallback<>(mapper, entityClass, collectionName), collectionName);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#findAllAndRemove(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.Class)
	 */
	@Override
	public <T> List<T> findAllAndRemove(NoSQLQuery<Document> query, Class<T> entityClass) {
		return findAllAndRemove(query, entityClass, operations.determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#findAllAndRemove(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> List<T> findAllAndRemove(NoSQLQuery<Document> query, Class<T> entityClass, String collectionName)
			 {
		return doFindAndDelete(collectionName, query, entityClass);
	}

	protected <T> List<T> doFindAndDelete(String collectionName, NoSQLQuery<Document> query, Class<T> entityClass)
			{

		List<T> result = find(query, entityClass, collectionName);

		if (!CollectionUtils.isEmpty(result)) {

			NoSQLQuery<Document> byIdInQuery = operations.getByIdInQuery(result);

			remove(byIdInQuery, entityClass, collectionName);
		}

		return result;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#findAllAndRemove(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.String)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> List<T> findAllAndRemove(NoSQLQuery<Document> query, String collectionName) {
		return (List<T>) findAllAndRemove(query, Object.class, collectionName);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#findAndModify(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, br.com.anteros.nosql.persistence.session.query.NoSQLUpdate, java.lang.Class)
	 */
	@Override
	public <T> T findAndModify(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, Class<T> entityClass){
		return findAndModify(query, update, new FindAndModifyOptions(), entityClass,
				operations.determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#findAndModify(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, br.com.anteros.nosql.persistence.session.query.NoSQLUpdate, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> T findAndModify(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, Class<T> entityClass,
			String collectionName) {
		return findAndModify(query, update, new FindAndModifyOptions(), entityClass, collectionName);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#findAndModify(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, br.com.anteros.nosql.persistence.session.query.NoSQLUpdate, br.com.anteros.nosql.persistence.session.FindAndModifyOptions, java.lang.Class)
	 */
	@Override
	public <T> T findAndModify(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, FindAndModifyOptions options,
			Class<T> entityClass)  {
		return findAndModify(query, update, options, entityClass, operations.determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#findAndModify(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, br.com.anteros.nosql.persistence.session.query.NoSQLUpdate, br.com.anteros.nosql.persistence.session.FindAndModifyOptions, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> T findAndModify(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, FindAndModifyOptions options,
			Class<T> entityClass, String collectionName) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(update, "Update must not be null!");
		Assert.notNull(options, "Options must not be null!");
		Assert.notNull(entityClass, "EntityClass must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");

		FindAndModifyOptions optionsToUse = FindAndModifyOptions.of(options);

		return doFindAndModify(collectionName, query.getQueryObject(), query.getFieldsObject(),
				getMappedSortObject(query, entityClass), entityClass, update, optionsToUse);
	}

	protected <T> T doFindAndRemove(String collectionName, Document query, Document fields, Document sort,
			Class<T> entityClass) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("findAndRemove using query: {} fields: {} sort: {} for class: {} in collection: {}",
					SerializationUtils.serializeToJsonSafely(query), fields, sort, entityClass, collectionName);
		}

		return executeFindOneInternal(new FindAndRemoveCallback(query, fields, sort),
				new ReadDocumentCallback<>(mapper, entityClass, collectionName), collectionName);
	}

	protected <T> T doFindAndModify(String collectionName, Document query, Document fields, Document sort,
			Class<T> entityClass, NoSQLUpdate<Document> update, FindAndModifyOptions options) {

		if (options == null) {
			options = new FindAndModifyOptions();
		}

		NoSQLDescriptionEntity descriptionEntity = entityClass == null ? null
				: descriptionEntityManager.getDescriptionEntity(entityClass);

		increaseVersionForUpdateIfNecessary(descriptionEntity, update);

		if (LOG.isDebugEnabled()) {
			LOG.debug(
					"findAndModify using query: {} fields: {} sort: {} for class: {} and update: {} "
							+ "in collection: {}",
					SerializationUtils.serializeToJsonSafely(query), fields, sort, entityClass,
					SerializationUtils.serializeToJsonSafely(update), collectionName);
		}

		return executeFindOneInternal(new FindAndModifyCallback(query, fields, sort, update.getUpdateObject(), options),
				new ReadDocumentCallback<>(mapper, entityClass, collectionName), collectionName);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#findAndReplace(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, S, br.com.anteros.nosql.persistence.session.FindAndReplaceOptions, java.lang.Class, java.lang.String, java.lang.Class)
	 */
	@Override
	public <S, T> T findAndReplace(NoSQLQuery<Document> query, S replacement, FindAndReplaceOptions options,
			Class<S> entityType, String collectionName, Class<T> resultType) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(replacement, "Replacement must not be null!");
		Assert.notNull(options, "Options must not be null! Use FindAndReplaceOptions#empty() instead.");
		Assert.notNull(entityType, "EntityType must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");
		Assert.notNull(resultType, "ResultType must not be null! Use Object.class instead.");

		Assert.isTrue(query.getLimit() <= 1, "Query must not define a limit other than 1 ore none!");
		Assert.isTrue(query.getOffset() <= 0, "Query must not define skip.");

		Document mappedQuery = query.getQueryObject();
		Document mappedFields = query.getFieldsObject();
		Document mappedSort = query.getSortObject();

		Document mappedReplacement = (Document) operations.forEntity(replacement).toMappedDocument().getDocument();

		return doFindAndReplace(collectionName, mappedQuery, mappedFields, mappedSort, null, entityType,
				mappedReplacement, options, resultType);
	}

	protected <T> T doFindAndReplace(String collectionName, Document mappedQuery, Document mappedFields,
			Document mappedSort, com.mongodb.client.model.Collation collation, Class<?> entityType,
			Document replacement, FindAndReplaceOptions options, Class<T> resultType) {

		if (LOG.isDebugEnabled()) {
			LOG.debug(
					"findAndReplace using query: {} fields: {} sort: {} for class: {} and replacement: {} "
							+ "in collection: {}",
					SerializationUtils.serializeToJsonSafely(mappedQuery),
					SerializationUtils.serializeToJsonSafely(mappedFields),
					SerializationUtils.serializeToJsonSafely(mappedSort), entityType,
					SerializationUtils.serializeToJsonSafely(replacement), collectionName);
		}

		maybeEmitEvent(new MongoBeforeSaveEvent(replacement, replacement, collectionName));

		return executeFindOneInternal(
				new FindAndReplaceCallback(mappedQuery, mappedFields, mappedSort, replacement, collation, options),
				new ProjectingReadCallback<>(mapper, entityType, resultType, collectionName), collectionName);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#findAndRemove(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.Class)
	 */
	@Override
	public <T> T findAndRemove(NoSQLQuery<Document> query, Class<T> entityClass) {
		return findAndRemove(query, entityClass, operations.determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#findAndRemove(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> T findAndRemove(NoSQLQuery<Document> query, Class<T> entityClass, String collectionName){

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(entityClass, "EntityClass must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");

		return doFindAndRemove(collectionName, query.getQueryObject(), query.getFieldsObject(),
				getMappedSortObject(query, entityClass), entityClass);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#findById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> T findById(Object id, Class<T> entityClass) {
		return findById(id, entityClass, operations.determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#findById(java.lang.Object, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> T findById(Object id, Class<T> entityClass, String collectionName) {
		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(entityClass, "EntityClass must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");

		String idKey = operations.getIdPropertyName(entityClass);
		Object newId = operations.convertIdProperty(entityClass,id);

		return doFindOne(collectionName, new Document(idKey, newId), new Document(), entityClass);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#findOne(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.Class)
	 */
	@Override
	public <T> T findOne(NoSQLQuery<Document> query, Class<T> entityClass) {
		return findOne(query, entityClass, operations.determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#findOne(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> T findOne(NoSQLQuery<Document> query, Class<T> entityClass, String collectionName) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(entityClass, "EntityClass must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");

		if (ObjectUtils.isEmpty(query.getSortObject())) {
			return doFindOne(collectionName, query.getQueryObject(), query.getFieldsObject(), entityClass);
		} else {
			query.limit(1);
			List<T> results = find(query, entityClass, collectionName);
			return results.isEmpty() ? null : results.get(0);
		}
	}

	protected <T> T doFindOne(String collectionName, Document query, Document fields, Class<T> entityClass) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("findOne using query: {} fields: {} for class: {} in collection: {}",
					SerializationUtils.serializeToJsonSafely(query), fields, entityClass, collectionName);
		}

		return executeFindOneInternal(new FindOneCallback(query, fields),
				new ReadDocumentCallback<>(this.mapper, entityClass, collectionName), collectionName);
	}

	private <T> T executeFindOneInternal(CollectionCallback<Document> collectionCallback,
			DocumentCallback<T> objectCallback, String collectionName) {
		try {
			T result = objectCallback
					.doWith(collectionCallback.doInCollection(getAndPrepareCollection(getDatabase(), collectionName)));
			return result;
		} catch (Exception e) {
			throw translateRuntimeException(e, exceptionTranslator);
		}
	}

	private MongoCollection<Document> getAndPrepareCollection(MongoDatabase db, String collectionName) {
		try {
			MongoCollection<Document> collection = db.getCollection(collectionName, Document.class);
			collection = prepareCollection(collection);
			return collection;
		} catch (RuntimeException e) {
			throw translateRuntimeException(e, exceptionTranslator);
		}
	}

	protected MongoCollection<Document> prepareCollection(MongoCollection<Document> collection) {

		if (this.readPreference != null) {
			collection = collection.withReadPreference(readPreference);
		}

		return collection;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#getCollectionName(java.lang.Class)
	 */
	@Override
	public String getCollectionName(Class<?> entityClass) {
		NoSQLDescriptionEntity descriptionEntity = getDescriptionEntity(entityClass);
		return descriptionEntity.getCollectionName();
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#getCollectionName(java.lang.Object)
	 */
	@Override
	public String getCollectionName(Object entity) {
		return null == entity ? null : getCollectionName(entity.getClass());
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#getCollectionNames()
	 */
	@Override
	public Set<String> getCollectionNames() {
		return execute(new DbCallback<Set<String>>() {
			public Set<String> doInDB(MongoDatabase db) throws MongoException, NoSQLDataAccessException {
				Set<String> result = new LinkedHashSet<>();
				for (String name : db.listCollectionNames()) {
					result.add(name);
				}
				return result;
			}
		});
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#group(br.com.anteros.nosql.persistence.session.query.NoSQLCriteria, java.lang.String, br.com.anteros.nosql.persistence.mongodb.session.GroupBy, java.lang.Class)
	 */
	@Override
	public <T> GroupByResults<T> group(NoSQLCriteria<Document> criteria, String inputCollectionName, GroupBy groupBy,
			Class<T> entityClass) throws Exception {
		Document document = groupBy.getGroupByObject();
		document.put("ns", inputCollectionName);

		Document query = criteria.getCriteriaObject();

		if (criteria == null) {
			document.put("cond", null);
		} else {
			document.put("cond", query);
		}
		// If initial document was a JavaScript string, potentially loaded by Spring's
		// Resource abstraction, load it and
		// convert to Document

		if (document.containsKey("initial")) {
			Object initialObj = document.get("initial");
			if (initialObj instanceof String) {
				String initialAsString = replaceWithResourceIfNecessary((String) initialObj);
				document.put("initial", Document.parse(initialAsString));
			}
		}

		if (document.containsKey("$reduce")) {
			document.put("$reduce",
					replaceWithResourceIfNecessary(ObjectUtils.nullSafeToString(document.get("$reduce"))));
		}
		if (document.containsKey("$keyf")) {
			document.put("$keyf", replaceWithResourceIfNecessary(ObjectUtils.nullSafeToString(document.get("$keyf"))));
		}
		if (document.containsKey("finalize")) {
			document.put("finalize",
					replaceWithResourceIfNecessary(ObjectUtils.nullSafeToString(document.get("finalize"))));
		}

		Document commandObject = new Document("group", document);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Executing Group with Document [{}]", SerializationUtils.serializeToJsonSafely(commandObject));
		}

		Document commandResult = executeCommand(commandObject, this.readPreference);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Group command result = [{}]", commandResult);
		}

		@SuppressWarnings("unchecked")
		Iterable<Document> resultSet = (Iterable<Document>) commandResult.get("retval");
		List<T> mappedResults = new ArrayList<>();
		DocumentCallback<T> callback = new ReadDocumentCallback<>(mapper, entityClass, inputCollectionName);

		for (Document resultDocument : resultSet) {
			mappedResults.add(callback.doWith(resultDocument));
		}

		return new GroupByResults<>(mappedResults, commandResult);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#group(java.lang.String, br.com.anteros.nosql.persistence.mongodb.session.GroupBy, java.lang.Class)
	 */
	@Override
	public <T> GroupByResults<T> group(String inputCollectionName, GroupBy groupBy, Class<T> entityClass)
			throws Exception {
		return group(null, inputCollectionName, groupBy, entityClass);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#insert(java.util.Collection, java.lang.Class)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> Collection<T> insert(Collection<? extends Object> batchToSave, Class<?> entityClass) throws Exception {
		Assert.notNull(batchToSave, "BatchToSave must not be null!");

		return (Collection<T>) doInsertBatch(operations.determineCollectionName(entityClass), batchToSave);

	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#insert(java.util.Collection, java.lang.String)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> Collection<T> insert(Collection<? extends Object> batchToSave, String collectionName) throws Exception {
		Assert.notNull(batchToSave, "BatchToSave must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");

		return (Collection<T>) doInsertBatch(collectionName, batchToSave);

	}

	@SuppressWarnings("unchecked")
	protected <T> Collection<T> doInsertBatch(String collectionName, Collection<? extends T> batchToSave)
			throws Exception {

		List<Document> documentList = new ArrayList<>();
		List<T> initializedBatchToSave = new ArrayList<>(batchToSave.size());
		for (T uninitialized : batchToSave) {

			NoSQLEntityAdapter<T> entity = operations.forEntity(uninitialized);
			T toSave = entity.initializeVersionProperty();

			toSave = (T) maybeEmitEvent(new MongoBeforeConvertEvent(toSave, new Document(), collectionName))
					.getSource();

			Document document = (Document) entity.toMappedDocument().getDocument();

			MongoBeforeSaveEvent beforeSaveEvent = new MongoBeforeSaveEvent(toSave, document, collectionName);

			maybeEmitEvent(beforeSaveEvent);
			documentList.add(document);
			initializedBatchToSave.add(toSave);
		}

		List<Object> ids = insertDocumentList(collectionName, documentList);
		List<T> savedObjects = new ArrayList<>(documentList.size());

		int i = 0;
		for (T obj : initializedBatchToSave) {

			if (i < ids.size()) {
				T saved = populateIdIfNecessary(obj, ids.get(i));
				maybeEmitEvent(new MongoAfterSaveEvent(saved, documentList.get(i), collectionName));
				savedObjects.add(saved);
			} else {
				savedObjects.add(obj);
			}
			i++;
		}

		return savedObjects;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#insert(T)
	 */
	@Override
	public <T> T insert(T objectToSave) throws Exception {
		Assert.notNull(objectToSave, "ObjectToSave must not be null!");

		ensureNotIterable(objectToSave);
		return insert(objectToSave, operations.determineEntityCollectionName(objectToSave));
	}

	protected void ensureNotIterable(Object o) {
		if (null != o) {
			if (o.getClass().isArray() || ITERABLE_CLASSES.contains(o.getClass().getName())) {
				throw new IllegalArgumentException("Cannot use a collection here.");
			}
		}
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#insert(T, java.lang.String)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> T insert(T objectToSave, String collectionName) throws Exception {

		Assert.notNull(objectToSave, "ObjectToSave must not be null!");
		Assert.notNull(collectionName, "CollectionName must not be null!");

		ensureNotIterable(objectToSave);

		NoSQLEntityAdapter<T> entity = operations.forEntity(objectToSave);
		T toSave = entity.initializeVersionProperty();

		MongoBeforeConvertEvent event = new MongoBeforeConvertEvent(toSave, new Document(), collectionName);
		toSave = (T) maybeEmitEvent(event).getSource();

		entity.assertUpdateableIdIfNotSet();

		Document dbDoc = (Document) entity.toMappedDocument().getDocument();

		maybeEmitEvent(new MongoBeforeSaveEvent(toSave, dbDoc, collectionName));
		Object id = insertDocument(collectionName, dbDoc, toSave.getClass());

		T saved = populateIdIfNecessary(toSave, id);
		maybeEmitEvent(new MongoAfterSaveEvent(saved, dbDoc, collectionName));

		return saved;

	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#insertAll(java.util.Collection)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> Collection<T> insertAll(Collection<? extends Object> objectsToSave) throws Exception {
		Assert.notNull(objectsToSave, "ObjectsToSave must not be null!");
		return (Collection<T>) doInsertAll(objectsToSave);
	}

	protected <T> Collection<T> doInsertAll(Collection<? extends T> listToSave) throws Exception {

		Map<String, List<T>> elementsByCollection = new HashMap<>();
		List<T> savedObjects = new ArrayList<>(listToSave.size());

		for (T element : listToSave) {

			if (element == null) {
				continue;
			}

			NoSQLDescriptionEntity entity = descriptionEntityManager.getDescriptionEntity(element.getClass());

			String collection = entity.getCollectionName();
			List<T> collectionElements = elementsByCollection.get(collection);

			if (null == collectionElements) {
				collectionElements = new ArrayList<>();
				elementsByCollection.put(collection, collectionElements);
			}

			collectionElements.add(element);
		}

		for (Map.Entry<String, List<T>> entry : elementsByCollection.entrySet()) {
			savedObjects.addAll((Collection<T>) doInsertBatch(entry.getKey(), entry.getValue()));
		}

		return savedObjects;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#mapReduce(java.lang.String, java.lang.String, java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> MapReduceResults<T> mapReduce(String inputCollectionName, String mapFunction, String reduceFunction,
			Class<T> entityClass) {
		return mapReduce(MongoQuery.of(), inputCollectionName, mapFunction, reduceFunction,
				new MapReduceOptions().outputTypeInline(), entityClass);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#mapReduce(java.lang.String, java.lang.String, java.lang.String, br.com.anteros.nosql.persistence.mongodb.session.MapReduceOptions, java.lang.Class)
	 */
	@Override
	public <T> MapReduceResults<T> mapReduce(String inputCollectionName, String mapFunction, String reduceFunction,
			MapReduceOptions mapReduceOptions, Class<T> entityClass) {
		return mapReduce(MongoQuery.of(), inputCollectionName, mapFunction, reduceFunction, mapReduceOptions,
				entityClass);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#mapReduce(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.String, java.lang.String, java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> MapReduceResults<T> mapReduce(NoSQLQuery<Document> query, String inputCollectionName, String mapFunction,
			String reduceFunction, Class<T> entityClass) {
		return mapReduce(query, inputCollectionName, mapFunction, reduceFunction,
				new MapReduceOptions().outputTypeInline(), entityClass);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#mapReduce(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.String, java.lang.String, java.lang.String, br.com.anteros.nosql.persistence.mongodb.session.MapReduceOptions, java.lang.Class)
	 */
	@Override
	public <T> MapReduceResults<T> mapReduce(NoSQLQuery<Document> query, String inputCollectionName, String mapFunction,
			String reduceFunction, MapReduceOptions mapReduceOptions, Class<T> entityClass) {

		return new MapReduceResults<>(mapReduce(query, entityClass, inputCollectionName, mapFunction, reduceFunction,
				mapReduceOptions, entityClass), new Document());
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#mapReduce(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.Class, java.lang.String, java.lang.String, java.lang.String, br.com.anteros.nosql.persistence.mongodb.session.MapReduceOptions, java.lang.Class)
	 */
	@Override
	public <T> List<T> mapReduce(NoSQLQuery<Document> query, Class<?> domainType, String inputCollectionName,
			String mapFunction, String reduceFunction, MapReduceOptions mapReduceOptions, Class<T> resultType) {

		Assert.notNull(domainType, "Domain type must not be null!");
		Assert.notNull(inputCollectionName, "Input collection name must not be null!");
		Assert.notNull(resultType, "Result type must not be null!");
		Assert.notNull(mapFunction, "Map function must not be null!");
		Assert.notNull(reduceFunction, "Reduce function must not be null!");

		String mapFunc = replaceWithResourceIfNecessary(mapFunction);
		String reduceFunc = replaceWithResourceIfNecessary(reduceFunction);
		MongoCollection<Document> inputCollection = getAndPrepareCollection(getDatabase(), inputCollectionName);

		// MapReduceOp
		MapReduceIterable<Document> mapReduce = inputCollection.mapReduce(mapFunc, reduceFunc, Document.class);

		if (query.getLimit() > 0 && mapReduceOptions != null && mapReduceOptions.getLimit() == null) {
			mapReduce = mapReduce.limit(query.getLimit());
		}
		if (((MongoQuery) query).getMeta().getMaxTimeMsec() != null) {
			mapReduce = mapReduce.maxTime(((MongoQuery) query).getMeta().getMaxTimeMsec(), TimeUnit.MILLISECONDS);
		}
		mapReduce = mapReduce.sort(getMappedSortObject(query, domainType));

		mapReduce = mapReduce.filter(query.getQueryObject());

		if (mapReduceOptions != null) {
			if (!CollectionUtils.isEmpty(mapReduceOptions.getScopeVariables())) {
				mapReduce = mapReduce.scope(new Document(mapReduceOptions.getScopeVariables()));
			}

			if (mapReduceOptions.getLimit() != null && mapReduceOptions.getLimit() > 0) {
				mapReduce = mapReduce.limit(mapReduceOptions.getLimit());
			}

			if (mapReduceOptions.getFinalizeFunction().filter(StringUtils::hasText).isPresent()) {
				mapReduce = mapReduce.finalizeFunction(mapReduceOptions.getFinalizeFunction().get());
			}

			if (mapReduceOptions.getJavaScriptMode() != null) {
				mapReduce = mapReduce.jsMode(mapReduceOptions.getJavaScriptMode());
			}

			if (mapReduceOptions.getOutputSharded().isPresent()) {
				mapReduce = mapReduce.sharded(mapReduceOptions.getOutputSharded().get());
			}

			if (StringUtils.hasText(mapReduceOptions.getOutputCollection()) && !mapReduceOptions.usesInlineOutput()) {

				mapReduce = mapReduce.collectionName(mapReduceOptions.getOutputCollection())
						.action(mapReduceOptions.getMapReduceAction());

				if (mapReduceOptions.getOutputDatabase().isPresent()) {
					mapReduce = mapReduce.databaseName(mapReduceOptions.getOutputDatabase().get());
				}
			}
		}

		List<T> mappedResults = new ArrayList<>();
		DocumentCallback<T> callback = new ReadDocumentCallback<>(mapper, resultType, inputCollectionName);

		for (Document document : mapReduce) {
			mappedResults.add(callback.doWith(document));
		}

		return mappedResults;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#remove(java.lang.Object)
	 */
	@Override
	public NoSQLResult remove(Object object) {

		Assert.notNull(object, "Object must not be null!");
		NoSQLQuery<Document> query = operations.forEntity(object).getByIdQuery();
		return remove(query, object.getClass());
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#remove(java.lang.Object, java.lang.String)
	 */
	@Override
	public NoSQLResult remove(Object object, String collectionName) throws Exception {
		Assert.notNull(object, "Object must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		NoSQLQuery<Document> query = operations.forEntity(object).getByIdQuery();

		return doRemove(collectionName, query, object.getClass(), false);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#remove(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.Class)
	 */
	@Override
	public <T> NoSQLResult remove(NoSQLQuery<Document> query, Class<?> entityClass) {
		return remove(query, entityClass, operations.determineCollectionName(entityClass));
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#remove(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> NoSQLResult remove(NoSQLQuery<Document> query, Class<?> entityClass, String collectionName)
			{
		Assert.notNull(entityClass, "EntityClass must not be null!");
		return doRemove(collectionName, query, entityClass, true);
	}

	protected <T> NoSQLResult doRemove(final String collectionName, final NoSQLQuery<Document> query,
			final Class<T> entityClass, boolean multi)  {

		Assert.notNull(query, "Query must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		final Document queryObject = query.getQueryObject();

		return execute(collectionName, new CollectionCallback<NoSQLResult>() {

			public NoSQLResult doInCollection(MongoCollection<Document> collection) {

				maybeEmitEvent(new MongoBeforeDeleteEvent(queryObject, entityClass, collectionName));

				Document removeQuery = queryObject;

				DeleteOptions options = new DeleteOptions();

				WriteConcern writeConcernToUse = defaultWriteConcern;

				if (LOG.isDebugEnabled()) {
					LOG.debug("Remove using query: {} in collection: {}.",
							new Object[] { SerializationUtils.serializeToJsonSafely(removeQuery), collectionName });
				}

				if (query.getLimit() > 0 || query.getOffset() > 0) {

					MongoCursor<Document> cursor = new QueryCursorPreparer(query, entityClass)
							.prepare(collection.find(removeQuery).projection(MongoMappedDocument.getIdOnlyProjection())) //
							.iterator();

					Set<Object> ids = new LinkedHashSet<>();
					while (cursor.hasNext()) {
						ids.add(MongoMappedDocument.of(cursor.next()).getId());
					}
					cursor.close();
					removeQuery = MongoMappedDocument.getIdIn(ids);
				}

				MongoCollection<Document> collectionToUse = writeConcernToUse != null
						? collection.withWriteConcern(writeConcernToUse)
						: collection;

				DeleteResult result = multi ? collectionToUse.deleteMany(removeQuery, options)
						: collection.deleteOne(removeQuery, options);

				maybeEmitEvent(new MongoAfterDeleteEvent(queryObject, entityClass, collectionName));

				return MongoDeleteResult.of(result);
			}
		});
	}

	protected List<Object> insertDocumentList(final String collectionName, final List<Document> documents)
			throws Exception {

		if (documents.isEmpty()) {
			return Collections.emptyList();
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Inserting list of Documents containing {} items", documents.size());
		}

		execute(collectionName, collection -> {

			WriteConcern writeConcernToUse = defaultWriteConcern;

			if (writeConcernToUse == null) {
				collection.insertMany(documents);
			} else {
				collection.withWriteConcern(writeConcernToUse).insertMany(documents);
			}

			return null;
		});

		return MongoMappedDocument.toIds(documents);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#remove(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, java.lang.String)
	 */
	@Override
	public <T> NoSQLResult remove(NoSQLQuery<Document> query, String collectionName) {
		MongoQuery mongoQuery = (MongoQuery) query;
		MongoCollection<Document> collection = getCollection(collectionName);
		return MongoDeleteResult.of(collection.deleteMany(mongoQuery.getQueryObject()));
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#getCollection(java.lang.String)
	 */
	@Override
	public MongoCollection<Document> getCollection(String collectionName) {
		return getDatabase().getCollection(collectionName);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#getCollection(java.lang.Class)
	 */
	@Override
	public MongoCollection<Document> getCollection(Class<?> entityClass) {
		return getDatabase()
				.getCollection(descriptionEntityManager.getDescriptionEntity(entityClass).getCollectionName());
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#save(T)
	 */
	@Override
	public <T> T save(T objectToSave) {
		Assert.notNull(objectToSave, "Object to save must not be null!");

		return save(objectToSave, operations.determineCollectionName(objectToSave.getClass()));
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#save(T, java.lang.String)
	 */
	@Override
	public <T> T save(T entity, String collectionName) {
		Assert.notNull(entity, "Object to save must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		NoSQLEntityAdapter<T> source = operations.forEntity(entity);

		return source.isVersionedEntity() ? doSaveVersioned(source, collectionName) : doSave(entity, collectionName);
	}

	@SuppressWarnings("unchecked")
	private <T> T doSave(T objectToSave, String collectionName) {

		NoSQLEntityAdapter<T> source = operations.forEntity(objectToSave);
		objectToSave = (T) maybeEmitEvent(new MongoBeforeConvertEvent(objectToSave, new Document(), collectionName))
				.getSource();

		NoSQLMappedDocument mappedEntity = source.toMappedDocument();
		Document dbDoc = (Document) mappedEntity.getDocument();

		maybeEmitEvent(new MongoBeforeSaveEvent(objectToSave, dbDoc, collectionName));
		Object id = saveDocument(collectionName, dbDoc, source.getEntity().getClass());

		T saved = source.populateIdIfNecessary(id);
		maybeEmitEvent(new MongoAfterSaveEvent(saved, dbDoc, collectionName));

		return saved;
	}

	protected Object saveDocument(final String collectionName, final Document dbDoc, final Class<?> entityClass) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Saving Document containing fields: {}", dbDoc.keySet());
		}

		return execute(collectionName, new CollectionCallback<Object>() {
			public Object doInCollection(MongoCollection<Document> collection) throws MongoException {
				MongoMappedDocument mapped = MongoMappedDocument.of(dbDoc);

				if (!mapped.hasId()) {
					if (defaultWriteConcern == null) {
						collection.insertOne(dbDoc);
					} else {
						collection.withWriteConcern(defaultWriteConcern).insertOne(dbDoc);
					}
				} else if (defaultWriteConcern == null) {
					collection.replaceOne(mapped.getIdFilter(), dbDoc, new ReplaceOptions().upsert(true));
				} else {
					collection.withWriteConcern(defaultWriteConcern).replaceOne(mapped.getIdFilter(), dbDoc,
							new ReplaceOptions().upsert(true));
				}
				return mapped.getId();
			}
		});
	}

	@SuppressWarnings("unchecked")
	private <T> T doSaveVersioned(NoSQLEntityAdapter<T> source, String collectionName) {
		Number version = (Number) source.getVersion();

		if (version != null) {

			// Create query for entity with the id and old version
			NoSQLQuery<Document> query = source.getQueryForVersion();

			// Bump version number
			T toSave = source.incrementVersion();

			toSave = (T) maybeEmitEvent(new MongoBeforeConvertEvent(toSave, new Document(), collectionName))
					.getSource();

			source.assertUpdateableIdIfNotSet();

			MongoMappedDocument mapped = (MongoMappedDocument) source.toMappedDocument();

			maybeEmitEvent(new MongoBeforeSaveEvent(toSave, mapped.getDocument(), collectionName));
			NoSQLUpdate<Document> update = mapped.updateWithoutId();

			NoSQLResult result = doUpdate(collectionName, query, update, toSave.getClass(), false, false);

			if (result.getModifiedOrDeletedCount() == 0) {
				throw new NoSQLOptimisticLockingFailureException(String.format(
						"Cannot save entity %s with version %s to collection %s. Has it been modified meanwhile?",
						source.getId(), version, collectionName));
			}
			maybeEmitEvent(new MongoAfterSaveEvent(toSave, mapped.getDocument(), collectionName));

			return toSave;
		}

		return (T) doInsert(collectionName, source.getEntity());
	}

	@SuppressWarnings("unchecked")
	protected <T> T doInsert(String collectionName, T objectToSave) {

		NoSQLEntityAdapter<T> entity = operations.forEntity(objectToSave);
		T toSave = entity.initializeVersionProperty();

		toSave = (T) maybeEmitEvent(new MongoBeforeConvertEvent(toSave, new Document(), collectionName)).getSource();

		entity.assertUpdateableIdIfNotSet();

		Document dbDoc = (Document) entity.toMappedDocument().getDocument();

		maybeEmitEvent(new MongoBeforeSaveEvent(toSave, dbDoc, collectionName));
		Object id = insertDocument(collectionName, dbDoc, toSave.getClass());

		T saved = populateIdIfNecessary(toSave, id);
		maybeEmitEvent(new MongoAfterSaveEvent(saved, dbDoc, collectionName));

		return saved;
	}

	protected Object insertDocument(final String collectionName, final Document document, final Class<?> entityClass) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Inserting Document containing fields: {} in collection: {}", document.keySet(), collectionName);
		}

		return execute(collectionName, new CollectionCallback<Object>() {
			public Object doInCollection(MongoCollection<Document> collection) throws NoSQLDataAccessException {
				if (defaultWriteConcern == null) {
					collection.insertOne(document);
				} else {
					collection.withWriteConcern(defaultWriteConcern).insertOne(document);
				}
				try {
					return operations.forEntity(document).getId();
				} catch (Exception ex) {
					throw translateRuntimeException(ex, exceptionTranslator);
				}
			}
		});
	}

	protected <T> NoSQLResult doUpdate(final String collectionName, final NoSQLQuery<Document> query,
			final NoSQLUpdate<Document> update, final Class<?> entityClass, final boolean upsert, final boolean multi)
			 {

		Assert.notNull(collectionName, "CollectionName must not be null!");
		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(update, "Update must not be null!");

		return execute(collectionName, new CollectionCallback<NoSQLResult>() {
			public NoSQLResult doInCollection(MongoCollection<Document> collection) throws NoSQLDataAccessException {

				NoSQLDescriptionEntity descriptionEntity = entityClass == null ? null
						: descriptionEntityManager.getDescriptionEntity(entityClass);

				increaseVersionForUpdateIfNecessary(descriptionEntity, update);

				UpdateOptions opts = new UpdateOptions();
				opts.upsert(upsert);

				Document queryObj = new Document();

				if (query != null) {
					queryObj.putAll(query.getQueryObject());
				}

				Document updateObj = (Document) update.getUpdateObject();

				if (multi && ((MongoUpdate) update).isIsolated() && !queryObj.containsKey("$isolated")) {
					queryObj.put("$isolated", 1);
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("Calling update using query: {} and update: {} in collection: {}",
							SerializationUtils.serializeToJsonSafely(queryObj),
							SerializationUtils.serializeToJsonSafely(updateObj), collectionName);
				}

				WriteConcern writeConcernToUse = defaultWriteConcern;

				collection = writeConcernToUse != null ? collection.withWriteConcern(writeConcernToUse) : collection;

				if (!isUpdateObject(updateObj)) {

					ReplaceOptions replaceOptions = new ReplaceOptions();
					replaceOptions.collation(opts.getCollation());
					replaceOptions.upsert(opts.isUpsert());

					return MongoUpdateResult.of(collection.replaceOne(queryObj, updateObj, replaceOptions));
				} else {
					if (multi) {
						return MongoUpdateResult.of(collection.updateMany(queryObj, updateObj, opts));
					} else {
						return MongoUpdateResult.of(collection.updateOne(queryObj, updateObj, opts));
					}
				}
			}
		});
	}

	public static boolean isUpdateObject(Document updateObj) {

		if (updateObj == null) {
			return false;
		}

		for (String s : updateObj.keySet()) {
			if (s.startsWith("$")) {
				return true;
			}
		}

		return false;
	}

	protected <T> T populateIdIfNecessary(T savedObject, Object id) {
		return operations.forEntity(savedObject).populateIdIfNecessary(id);
	}

	private void increaseVersionForUpdateIfNecessary(NoSQLDescriptionEntity descriptionEntity,
			NoSQLUpdate<Document> update) {

		if (descriptionEntity != null && descriptionEntity.hasVersionProperty()) {
			String versionFieldName = descriptionEntity.getDescriptionVersionField().getName();
			if (!((MongoUpdate) update).modifies(versionFieldName)) {
				((MongoUpdate) update).inc(versionFieldName, 1L);
			}
		}
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#updateFirst(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, br.com.anteros.nosql.persistence.session.query.NoSQLUpdate, java.lang.Class)
	 */
	@Override
	public <T> NoSQLResult updateFirst(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, Class<?> entityClass)
			throws Exception {
		return doUpdate(operations.determineCollectionName(entityClass), query, update, entityClass, false, false);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#updateFirst(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, br.com.anteros.nosql.persistence.session.query.NoSQLUpdate, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> NoSQLResult updateFirst(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, Class<?> entityClass,
			String collectionName) throws Exception {
		Assert.notNull(entityClass, "EntityClass must not be null!");

		return doUpdate(collectionName, query, update, entityClass, false, false);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#updateFirst(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, br.com.anteros.nosql.persistence.session.query.NoSQLUpdate, java.lang.String)
	 */
	@Override
	public <T> NoSQLResult updateFirst(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, String collectionName)
			throws Exception {
		return doUpdate(collectionName, query, update, null, false, false);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#updateMulti(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, br.com.anteros.nosql.persistence.session.query.NoSQLUpdate, java.lang.Class)
	 */
	@Override
	public <T> NoSQLResult updateMulti(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, Class<?> entityClass)
			throws Exception {
		return doUpdate(operations.determineCollectionName(entityClass), query, update, entityClass, false, true);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#updateMulti(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, br.com.anteros.nosql.persistence.session.query.NoSQLUpdate, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> NoSQLResult updateMulti(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, Class<?> entityClass,
			String collectionName) throws Exception {
		Assert.notNull(entityClass, "EntityClass must not be null!");

		return doUpdate(collectionName, query, update, entityClass, false, true);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#updateMulti(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, br.com.anteros.nosql.persistence.session.query.NoSQLUpdate, java.lang.String)
	 */
	@Override
	public <T> NoSQLResult updateMulti(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, String collectionName)
			throws Exception {
		return doUpdate(collectionName, query, update, null, false, true);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#upsert(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, br.com.anteros.nosql.persistence.session.query.NoSQLUpdate, java.lang.Class)
	 */
	@Override
	public <T> NoSQLResult upsert(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, Class<?> entityClass)
			throws Exception {
		return doUpdate(operations.determineCollectionName(entityClass), query, update, entityClass, true, false);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#upsert(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, br.com.anteros.nosql.persistence.session.query.NoSQLUpdate, java.lang.Class, java.lang.String)
	 */
	@Override
	public <T> NoSQLResult upsert(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, Class<?> entityClass,
			String collectionName) throws Exception {
		Assert.notNull(entityClass, "EntityClass must not be null!");

		return doUpdate(collectionName, query, update, entityClass, true, false);

	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#upsert(br.com.anteros.nosql.persistence.session.query.NoSQLQuery, br.com.anteros.nosql.persistence.session.query.NoSQLUpdate, java.lang.String)
	 */
	@Override
	public <T> NoSQLResult upsert(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, String collectionName)
			throws Exception {
		return doUpdate(collectionName, query, update, null, true, false);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#getKey(T)
	 */
	@Override
	public <T> Key<T> getKey(T entity) {
		return mapper.getKey(entity);
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#mapper()
	 */
	@Override
	public AbstractNoSQLObjectMapper mapper() {
		return mapper;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#getDefaultWriteConcern()
	 */
	@Override
	public WriteConcern getDefaultWriteConcern() {
		return defaultWriteConcern;
	}

	class QueryCursorPreparer implements CursorPreparer {

		private final NoSQLQuery<Document> query;
		private final Class<?> type;

		public QueryCursorPreparer(NoSQLQuery<Document> query, Class<?> type) {

			this.query = query;
			this.type = type;
		}

		public FindIterable<Document> prepare(FindIterable<Document> cursor) {

			if (query == null) {
				return cursor;
			}

			if (query.getOffset() <= 0 && query.getLimit() <= 0 && ObjectUtils.isEmpty(query.getSortObject())
					&& !StringUtils.hasText(query.getHint())) {
				return cursor;
			}

			FindIterable<Document> cursorToUse;

			cursorToUse = cursor; // falta colation

			try {
				if (query.getOffset() > 0) {
					cursorToUse = cursorToUse.skip((int) query.getOffset());
				}
				if (query.getLimit() > 0) {
					cursorToUse = cursorToUse.limit(query.getLimit());
				}
				if (!ObjectUtils.isEmpty(query.getSortObject())) {
					Document sort = type != null ? getMappedSortObject(query, type) : (Document) query.getSortObject();
					cursorToUse = cursorToUse.sort(sort);
				}

				if (StringUtils.hasText(query.getHint())) {
					cursorToUse = cursorToUse.hint(Document.parse(query.getHint()));
				}

			} catch (RuntimeException e) {
				throw translateRuntimeException(e, exceptionTranslator);
			}

			return cursorToUse;
		}
	}

	private static class FindOneCallback implements CollectionCallback<Document> {

		private final Document query;
		private final Optional<Document> fields;

		public FindOneCallback(Document query, Document fields) {
			this.query = query;
			this.fields = Optional.of(fields).filter(it -> !ObjectUtils.isEmpty(fields));
		}

		public Document doInCollection(MongoCollection<Document> collection) throws NoSQLDataAccessException {

			FindIterable<Document> iterable = collection.find(query, Document.class);

			if (LOG.isDebugEnabled()) {

				LOG.debug("findOne using query: {} fields: {} in db.collection: {}",
						SerializationUtils.serializeToJsonSafely(query),
						SerializationUtils.serializeToJsonSafely(fields.orElseGet(Document::new)),
						collection.getNamespace() != null ? collection.getNamespace().getFullName() : "n/a");
			}

			if (fields.isPresent()) {
				iterable = iterable.projection(fields.get());
			}

			return iterable.first();
		}
	}

	interface DocumentCallback<T> {

		T doWith(Document document);
	}

	private class ReadDocumentCallback<T> implements DocumentCallback<T> {

		private final AbstractNoSQLObjectMapper mapper;
		private final Class<T> entityClass;
		private final String collectionName;

		public ReadDocumentCallback(AbstractNoSQLObjectMapper mapper, Class<T> entityClass, String collectionName) {
			this.mapper = mapper;
			this.entityClass = entityClass;
			this.collectionName = collectionName;
		}

		public T doWith(Document document) {

			try {
				if (null != document) {
					maybeEmitEvent(new MongoAfterLoadEvent(document, entityClass, collectionName));
				}

				T source = mapper.fromDocument(SimpleMongoSession.this, entityClass, document, cache);

				if (null != source) {
					maybeEmitEvent(new MongoAfterConvertEvent(source, document, collectionName));
				}

				return source;
			} catch (Exception e) {
				throw translateRuntimeException(e, exceptionTranslator);
			}

		}
	}

	protected <T> NoSQLEvent<Document> maybeEmitEvent(NoSQLEvent<Document> event) {

		if (null != eventPublisher) {
			eventPublisher.publishEvent(event);
		}

		return event;
	}

	private Document getMappedSortObject(NoSQLQuery<Document> query, Class<?> type) {

		if (query == null || ObjectUtils.isEmpty(query.getSortObject())) {
			return null;
		}

		return query.getSortObject();
	}

	private static class FindCallback implements CollectionCallback<FindIterable<Document>> {

		private final Document query;
		private final Document fields;

		public FindCallback(Document query, Document fields) {

			Assert.notNull(query, "Query must not be null!");
			Assert.notNull(fields, "Fields must not be null!");

			this.query = query;
			this.fields = fields;
		}

		public FindIterable<Document> doInCollection(MongoCollection<Document> collection)
				throws NoSQLDataAccessException {
			return collection.find(query, Document.class).projection(fields);
		}
	}

	private <T> List<T> executeFindMultiInternal(CollectionCallback<FindIterable<Document>> collectionCallback,
			CursorPreparer preparer, DocumentCallback<T> objectCallback, String collectionName) {

		MongoCursor<Document> cursor = null;
		try {
			try {
				FindIterable<Document> iterable = collectionCallback
						.doInCollection(getAndPrepareCollection(getDatabase(), collectionName));

				if (preparer != null) {
					iterable = preparer.prepare(iterable);
				}
				cursor = iterable.iterator();
				List<T> result = new ArrayList<>();
				while (cursor.hasNext()) {
					Document object = cursor.next();
					result.add(objectCallback.doWith(object));
				}
				return result;
			} finally {

				if (cursor != null) {
					cursor.close();
				}
			}
		} catch (Exception e) {
			throw translateRuntimeException(e, exceptionTranslator);
		}

	}

	private static class ExistsCallback implements CollectionCallback<Boolean> {

		private final Document mappedQuery;

		public ExistsCallback(Document mappedQuery) {
			this.mappedQuery = mappedQuery;
		}

		@SuppressWarnings("deprecation")
		@Override
		public Boolean doInCollection(MongoCollection<Document> collection)
				throws MongoException, NoSQLDataAccessException {
			return collection.count(mappedQuery, new CountOptions().limit(1)) > 0;
		}
	}

	static RuntimeException translateRuntimeException(Exception ex, NoSQLExceptionTranslator exceptionTranslator) {
		RuntimeException resolved = exceptionTranslator.translateExceptionIfPossible(ex);
		return resolved == null ? new RuntimeException(ex) : resolved;
	}

	private static class FindAndModifyCallback implements CollectionCallback<Document> {

		private final Document query;
		private final Document fields;
		private final Document sort;
		private final Document update;
		private final FindAndModifyOptions options;

		public FindAndModifyCallback(Document query, Document fields, Document sort, Document update,
				FindAndModifyOptions options) {
			this.query = query;
			this.fields = fields;
			this.sort = sort;
			this.update = update;
			this.options = options;
		}

		public Document doInCollection(MongoCollection<Document> collection)
				throws MongoException, NoSQLDataAccessException {

			FindOneAndUpdateOptions opts = new FindOneAndUpdateOptions();
			opts.sort(sort);
			if (options.isUpsert()) {
				opts.upsert(true);
			}
			opts.projection(fields);
			if (options.isReturnNew()) {
				opts.returnDocument(ReturnDocument.AFTER);
			}

			return collection.findOneAndUpdate(query, update, opts);
		}
	}

	private static class FindAndReplaceCallback implements CollectionCallback<Document> {

		private final Document query;
		private final Document fields;
		private final Document sort;
		private final Document update;
		private final FindAndReplaceOptions options;

		FindAndReplaceCallback(Document query, Document fields, Document sort, Document update,
				com.mongodb.client.model.Collation collation, FindAndReplaceOptions options) {

			this.query = query;
			this.fields = fields;
			this.sort = sort;
			this.update = update;
			this.options = options;
		}

		@Override
		public Document doInCollection(MongoCollection<Document> collection)
				throws MongoException, NoSQLDataAccessException {

			FindOneAndReplaceOptions opts = new FindOneAndReplaceOptions();
			opts.sort(sort);
			opts.projection(fields);

			if (options.isUpsert()) {
				opts.upsert(true);
			}

			if (options.isReturnNew()) {
				opts.returnDocument(ReturnDocument.AFTER);
			}

			return collection.findOneAndReplace(query, update, opts);
		}
	}

	private class ProjectingReadCallback<S, T> implements DocumentCallback<T> {

		private final AbstractNoSQLObjectMapper mapper;
		private final Class<S> entityType;
		private final Class<T> targetType;
		private final String collectionName;

		public ProjectingReadCallback(AbstractNoSQLObjectMapper mapper, Class<S> entityType, Class<T> targetType,
				String collectionName) {
			super();
			this.mapper = mapper;
			this.entityType = entityType;
			this.targetType = targetType;
			this.collectionName = collectionName;
		}

		@SuppressWarnings("unchecked")
		public T doWith(Document document) {

			try {
				if (document == null) {
					return null;
				}

				Class<?> typeToRead = targetType.isInterface() || targetType.isAssignableFrom(entityType) ? entityType
						: targetType;

				if (null != document) {
					maybeEmitEvent(new MongoAfterLoadEvent(document, targetType, collectionName));
				}

				Object source = mapper.fromDocument(SimpleMongoSession.this, typeToRead, document, cache);
				Object result = source;// targetType.isInterface() ? projectionFactory.createProjection(targetType,
										// source) : source; //VER AQUI

				if (null != result) {
					maybeEmitEvent(new MongoAfterConvertEvent(result, document, collectionName));
				}

				return (T) result;
			} catch (Exception e) {
				throw translateRuntimeException(e, exceptionTranslator);
			}
		}
	}

	class UnwrapAndReadDocumentCallback<T> extends ReadDocumentCallback<T> {

		public UnwrapAndReadDocumentCallback(AbstractNoSQLObjectMapper mapper, Class<T> type, String collectionName) {
			super(mapper, type, collectionName);
		}

		@Override
		public T doWith(Document object) {

			try {
				if (object == null) {
					return null;
				}

				Object idField = object.get(UNDERSCORE_ID);

				if (!(idField instanceof Document)) {
					return super.doWith(object);
				}

				Document toMap = new Document();
				Document nested = (Document) idField;
				toMap.putAll(nested);

				for (String key : object.keySet()) {
					if (!UNDERSCORE_ID.equals(key)) {
						toMap.put(key, object.get(key));
					}
				}

				return super.doWith(toMap);
			} catch (Exception e) {
				throw translateRuntimeException(e, exceptionTranslator);
			}
		}
	}

	private static class FindAndRemoveCallback implements CollectionCallback<Document> {

		private final Document query;
		private final Document fields;
		private final Document sort;

		public FindAndRemoveCallback(Document query, Document fields, Document sort) {

			this.query = query;
			this.fields = fields;
			this.sort = sort;
		}

		public Document doInCollection(MongoCollection<Document> collection)
				throws MongoException, NoSQLDataAccessException {

			FindOneAndDeleteOptions opts = new FindOneAndDeleteOptions().sort(sort).projection(fields);

			return collection.findOneAndDelete(query, opts);
		}
	}

	protected String replaceWithResourceIfNecessary(String function) {

		String func = function;
		if (ResourceUtils.isUrl(function)) {
			InputStream resourceAsStream=null;
			try {
				resourceAsStream = ResourceUtils.getResourceAsStream(function);
				if (resourceAsStream != null) {
					Scanner scanner = null;
					try {
						scanner = new Scanner(resourceAsStream);
						return scanner.useDelimiter("\\A").next();
					} finally {
						if (scanner != null) {
							scanner.close();
						}
					}
					
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			} finally {
				if (resourceAsStream !=null) {
					try {
						resourceAsStream.close();
					} catch (IOException e) {
					}
				}
			}
		}

		return func;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#setEventPublisher(br.com.anteros.nosql.persistence.session.event.NoSQLEventPublisher)
	 */
	@Override
	public void setEventPublisher(NoSQLEventPublisher<Document> eventPublisher) {
		this.eventPublisher = eventPublisher;
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#commitTransaction()
	 */
	@Override
	public void commitTransaction() {
		try {
			this.getTransaction().commit();
		} catch (Exception e) {
			throw new NoSQLTransactionException(e);
		}
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#startTransaction()
	 */
	@Override
	public void startTransaction() {
		this.getTransaction().begin();
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#abortTransaction()
	 */
	@Override
	public void abortTransaction() {
		this.getTransaction().rollback();
	}

	/* (non-Javadoc)
	 * @see br.com.anteros.nosql.persistence.mongodb.session.MongoSession#count(java.lang.Class)
	 */
	@Override
	public <T> long count(Class<T> entityClass) {
		return this.getCollection(entityClass).count();
	}

	@Override
	public boolean isWithoutTransactionControl() {
		return withoutTransactionControl;
	}

	public void setWithoutTransactionControl(boolean withoutTransactionControl) {
		this.withoutTransactionControl = withoutTransactionControl;
	}

}
