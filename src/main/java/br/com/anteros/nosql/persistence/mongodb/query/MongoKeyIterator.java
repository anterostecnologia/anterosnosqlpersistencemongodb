package br.com.anteros.nosql.persistence.mongodb.query;

import java.util.Iterator;

import org.bson.Document;

import com.mongodb.DBObject;

import br.com.anteros.nosql.persistence.converters.Key;
import br.com.anteros.nosql.persistence.session.NoSQLSession;
import br.com.anteros.nosql.persistence.session.mapping.AbstractNoSQLObjectMapper;


public class MongoKeyIterator<T> extends MongoIterator<T, Key<T>> {
 
    public MongoKeyIterator(final NoSQLSession<?> session, final Iterator<Document> cursor, final AbstractNoSQLObjectMapper mapper,
                              final Class<T> clazz, final String collection) {
        super(session, cursor, mapper, clazz, collection, null);
    }

    @Override
    protected Key<T> convertItem(final Document dbObj) throws Exception {
        Object id = dbObj.get(AbstractNoSQLObjectMapper.ID_KEY);
        if (id instanceof DBObject) {
        	
            Class<?> type = getSession().getDescriptionEntityManager().getDescriptionEntity(getClazz()).getDescriptionIdField().getType();
            id = getMapper().fromDocument(getSession(), type, (DBObject) id, getMapper().createEntityCache());
        }
        return new Key<T>(getClazz(), getCollection(), id);
    }
}
