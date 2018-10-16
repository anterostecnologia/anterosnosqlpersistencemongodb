package br.com.anteros.nosql.persistence.mongodb.query;


import java.util.Iterator;
import java.util.NoSuchElementException;

import org.bson.Document;

import com.mongodb.client.MongoCursor;

import br.com.anteros.nosql.persistence.session.NoSQLSession;
import br.com.anteros.nosql.persistence.session.cache.NoSQLEntityCache;
import br.com.anteros.nosql.persistence.session.mapping.AbstractNoSQLObjectMapper;


public class MongoIterator<T, V> implements Iterable<V>, Iterator<V> {
    private final Iterator<Document> wrapped;
    private final AbstractNoSQLObjectMapper mapper;
    private final Class<T> clazz;
    private final String collection;
    private final NoSQLEntityCache cache;
    private long driverTime;
    private long mapperTime;
    private NoSQLSession<?> session;


    public MongoIterator(final NoSQLSession<?> session, final Iterator<Document> it, final AbstractNoSQLObjectMapper mapper, final Class<T> clazz,
                           final String collection, final NoSQLEntityCache cache) {
        wrapped = it;
        this.mapper = mapper;
        this.clazz = clazz;
        this.collection = collection;
        this.cache = cache;
        this.session = session;
    }

  
    public void close() {
        if (wrapped != null && wrapped instanceof MongoCursor) {
            ((MongoCursor<Document>) wrapped).close();
        }
    }


    public Class<T> getClazz() {
        return clazz;
    }

    public String getCollection() {
        return collection;
    }

    public MongoCursor<Document> getCursor() {
        return (MongoCursor<Document>) wrapped;
    }

 
    public long getDriverTime() {
        return driverTime;
    }

  
    public AbstractNoSQLObjectMapper getMapper() {
        return mapper;
    }

    
    public long getMapperTime() {
        return mapperTime;
    }

    @Override
    public boolean hasNext() {
        if (wrapped == null) {
            return false;
        }
        final long start = System.currentTimeMillis();
        final boolean ret = wrapped.hasNext();
        driverTime += System.currentTimeMillis() - start;
        return ret;
    }

    @Override
    public V next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final Document dbObj = getNext();
        try {
			return processItem(dbObj);
		} catch (Exception e) {
			throw new RuntimeException(e); 
		}
    }

    @Override
    public void remove() {
        final long start = System.currentTimeMillis();
        wrapped.remove();
        driverTime += System.currentTimeMillis() - start;
    }

    @Override
    public Iterator<V> iterator() {
        return this;
    }

    @SuppressWarnings("unchecked")
    protected V convertItem(final Document dbObj) throws Exception {
        return (V) mapper.fromDocument(session, clazz, dbObj, cache);
    }

    protected Document getNext() {
        final long start = System.currentTimeMillis();
        final Document dbObj = wrapped.next();
        driverTime += System.currentTimeMillis() - start;
        return dbObj;
    }

    private V processItem(final Document dbObj) throws Exception {
        final long start = System.currentTimeMillis();
        final V item = convertItem(dbObj);
        mapperTime += System.currentTimeMillis() - start;
        return item;
    }

    public NoSQLSession<?> getSession() {
        return session;
    }
}
