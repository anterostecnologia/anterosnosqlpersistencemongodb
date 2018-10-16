package br.com.anteros.nosql.persistence.mongodb.session;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.bson.Document;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import br.com.anteros.nosql.persistence.client.NoSQLConnection;
import br.com.anteros.nosql.persistence.converters.Key;
import br.com.anteros.nosql.persistence.dialect.NoSQLDialect;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionEntityManager;
import br.com.anteros.nosql.persistence.mongodb.aggregation.AggregationPipeline;
import br.com.anteros.nosql.persistence.session.FindAndModifyOptions;
import br.com.anteros.nosql.persistence.session.FindAndReplaceOptions;
import br.com.anteros.nosql.persistence.session.NoSQLPersistenceContext;
import br.com.anteros.nosql.persistence.session.NoSQLResult;
import br.com.anteros.nosql.persistence.session.NoSQLSession;
import br.com.anteros.nosql.persistence.session.NoSQLSessionFactory;
import br.com.anteros.nosql.persistence.session.NoSQLSessionListener;
import br.com.anteros.nosql.persistence.session.event.NoSQLEventPublisher;
import br.com.anteros.nosql.persistence.session.mapping.AbstractNoSQLObjectMapper;
import br.com.anteros.nosql.persistence.session.query.NoSQLCriteria;
import br.com.anteros.nosql.persistence.session.query.NoSQLQuery;
import br.com.anteros.nosql.persistence.session.query.NoSQLUpdate;
import br.com.anteros.nosql.persistence.session.transaction.NoSQLTransaction;

public interface MongoSession extends NoSQLSession<Document>{

	String UNDERSCORE_ID = "_id";
	String UNDERSCORE_ID_REF = "$_id";

	void flush() throws Exception;

	void forceFlush(Set<String> collectionNames) throws Exception;

	void onBeforeExecuteCommit(NoSQLConnection connection) throws Exception;

	void onBeforeExecuteRollback(NoSQLConnection connection) throws Exception;

	void onAfterExecuteCommit(NoSQLConnection connection) throws Exception;

	void onAfterExecuteRollback(NoSQLConnection connection) throws Exception;

	NoSQLDescriptionEntityManager getDescriptionEntityManager();

	NoSQLDialect getDialect();

	NoSQLConnection getConnection();

	NoSQLPersistenceContext getPersistenceContext();

	void addListener(NoSQLSessionListener listener);

	void removeListener(NoSQLSessionListener listener);

	List<NoSQLSessionListener> getListeners();

	void setFormatCommands(boolean format);

	boolean isShowCommands();

	boolean isFormatCommands();

	MongoDatabase getDatabase();

	MongoDatabase getDatabase(String dbName);

	void dropCollection(String collectionName);

	boolean isProxyObject(Object object) throws Exception;

	boolean proxyIsInitialized(Object object) throws Exception;

	<T> T cloneEntityManaged(Object object) throws Exception;

	void evict(Class<?> object);

	void evictAll();

	boolean isClosed() throws Exception;

	NoSQLTransaction getTransaction();

	NoSQLSessionFactory getNoSQLSessionFactory();

	void clear() throws Exception;

	boolean validationIsActive();

	void activateValidation();

	void deactivateValidation();

	void close() throws Exception;

	<T> Object getIdentifier(T owner) throws Exception;

	<T> boolean collectionExists(Class<T> entityClass);

	boolean collectionExists(String collectionName);

	AggregationPipeline createAggregation(Class<?> source);

	AggregationPipeline createAggregation(String collection, Class<?> clazz);

	<T> long count(NoSQLQuery<Document> query, Class<?> entityClass);

	<T> long count(NoSQLQuery<Document> query, Class<?> entityClass, String collectionName);

	<T> long count(NoSQLQuery<Document> query, String collectionName);

	<T> MongoCollection<Document> createCollection(Class<T> entityClass) throws Exception;

	MongoCollection<Document> createCollection(String collectionName) throws Exception;

	<T> void dropCollection(Class<T> entityClass);

	<T> T execute(Class<?> entityClass, CollectionCallback<T> callback) throws Exception;

	<T> T execute(DbCallback<T> action);

	<T> T execute(String collectionName, CollectionCallback<T> callback);

	Document executeCommand(Document command) throws Exception;

	Document executeCommand(Document command, ReadPreference readPreference) throws Exception;

	Document executeCommand(String jsonCommand) throws Exception;

	void executeQuery(NoSQLQuery<Document> query, String collectionName, DocumentCallbackHandler dch) throws Exception;

	<T> boolean exists(NoSQLQuery<Document> query, Class<?> entityClass);

	<T> boolean exists(NoSQLQuery<Document> query, Class<?> entityClass, String collectionName);

	<T> boolean exists(NoSQLQuery<Document> query, String collectionName);

	<T> List<T> find(NoSQLQuery<Document> query, Class<T> entityClass);

	<T> List<T> find(NoSQLQuery<Document> query, Class<T> entityClass, String collectionName);

	<T> List<T> findAll(Class<T> entityClass);

	<T> List<T> findAll(Class<T> entityClass, String collectionName);

	<T> List<T> findAllAndRemove(NoSQLQuery<Document> query, Class<T> entityClass);

	<T> List<T> findAllAndRemove(NoSQLQuery<Document> query, Class<T> entityClass, String collectionName);

	<T> List<T> findAllAndRemove(NoSQLQuery<Document> query, String collectionName);

	<T> T findAndModify(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, Class<T> entityClass);

	<T> T findAndModify(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, Class<T> entityClass,
			String collectionName);

	<T> T findAndModify(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, FindAndModifyOptions options,
			Class<T> entityClass);

	<T> T findAndModify(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, FindAndModifyOptions options,
			Class<T> entityClass, String collectionName);

	<S, T> T findAndReplace(NoSQLQuery<Document> query, S replacement, FindAndReplaceOptions options,
			Class<S> entityType, String collectionName, Class<T> resultType);

	<T> T findAndRemove(NoSQLQuery<Document> query, Class<T> entityClass);

	<T> T findAndRemove(NoSQLQuery<Document> query, Class<T> entityClass, String collectionName);

	<T> T findById(Object id, Class<T> entityClass);

	<T> T findById(Object id, Class<T> entityClass, String collectionName);

	<T> T findOne(NoSQLQuery<Document> query, Class<T> entityClass);

	<T> T findOne(NoSQLQuery<Document> query, Class<T> entityClass, String collectionName);

	String getCollectionName(Class<?> entityClass);

	String getCollectionName(Object entity);

	Set<String> getCollectionNames();

	<T> GroupByResults<T> group(NoSQLCriteria<Document> criteria, String inputCollectionName, GroupBy groupBy,
			Class<T> entityClass) throws Exception;

	<T> GroupByResults<T> group(String inputCollectionName, GroupBy groupBy, Class<T> entityClass) throws Exception;

	<T> Collection<T> insert(Collection<? extends Object> batchToSave, Class<?> entityClass) throws Exception;

	<T> Collection<T> insert(Collection<? extends Object> batchToSave, String collectionName) throws Exception;

	<T> T insert(T objectToSave) throws Exception;

	<T> T insert(T objectToSave, String collectionName) throws Exception;

	<T> Collection<T> insertAll(Collection<? extends Object> objectsToSave) throws Exception;

	<T> MapReduceResults<T> mapReduce(String inputCollectionName, String mapFunction, String reduceFunction,
			Class<T> entityClass);

	<T> MapReduceResults<T> mapReduce(String inputCollectionName, String mapFunction, String reduceFunction,
			MapReduceOptions mapReduceOptions, Class<T> entityClass);

	<T> MapReduceResults<T> mapReduce(NoSQLQuery<Document> query, String inputCollectionName, String mapFunction,
			String reduceFunction, Class<T> entityClass);

	<T> MapReduceResults<T> mapReduce(NoSQLQuery<Document> query, String inputCollectionName, String mapFunction,
			String reduceFunction, MapReduceOptions mapReduceOptions, Class<T> entityClass);

	<T> List<T> mapReduce(NoSQLQuery<Document> query, Class<?> domainType, String inputCollectionName,
			String mapFunction, String reduceFunction, MapReduceOptions mapReduceOptions, Class<T> resultType);

	NoSQLResult remove(Object object);

	NoSQLResult remove(Object object, String collectionName) throws Exception;

	<T> NoSQLResult remove(NoSQLQuery<Document> query, Class<?> entityClass);

	<T> NoSQLResult remove(NoSQLQuery<Document> query, Class<?> entityClass, String collectionName);

	<T> NoSQLResult remove(NoSQLQuery<Document> query, String collectionName);

	MongoCollection<Document> getCollection(String collectionName);

	MongoCollection<Document> getCollection(Class<?> entityClass);

	<T> T save(T objectToSave);

	<T> T save(T entity, String collectionName);

	<T> NoSQLResult updateFirst(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, Class<?> entityClass)
			throws Exception;

	<T> NoSQLResult updateFirst(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, Class<?> entityClass,
			String collectionName) throws Exception;

	<T> NoSQLResult updateFirst(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, String collectionName)
			throws Exception;

	<T> NoSQLResult updateMulti(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, Class<?> entityClass)
			throws Exception;

	<T> NoSQLResult updateMulti(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, Class<?> entityClass,
			String collectionName) throws Exception;

	<T> NoSQLResult updateMulti(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, String collectionName)
			throws Exception;

	<T> NoSQLResult upsert(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, Class<?> entityClass)
			throws Exception;

	<T> NoSQLResult upsert(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, Class<?> entityClass,
			String collectionName) throws Exception;

	<T> NoSQLResult upsert(NoSQLQuery<Document> query, NoSQLUpdate<Document> update, String collectionName)
			throws Exception;

	<T> Key<T> getKey(T entity);

	AbstractNoSQLObjectMapper mapper();

	WriteConcern getDefaultWriteConcern();

	void setEventPublisher(NoSQLEventPublisher<Document> eventPublisher);

	void commitTransaction();

	void startTransaction();

	void abortTransaction();

	<T> long count(Class<T> entityClass);
	
	public boolean isWithoutTransactionControl();

}