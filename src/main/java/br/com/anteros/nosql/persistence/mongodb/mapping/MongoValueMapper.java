package br.com.anteros.nosql.persistence.mongodb.mapping;


import java.util.Map;

import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;
import br.com.anteros.nosql.persistence.session.NoSQLSession;
import br.com.anteros.nosql.persistence.session.cache.NoSQLEntityCache;
import br.com.anteros.nosql.persistence.session.mapping.AbstractNoSQLObjectMapper;
import br.com.anteros.nosql.persistence.session.mapping.NoSQLCustomMapper;


class MongoValueMapper implements NoSQLCustomMapper {
	
    @Override
    public void fromDocument(final NoSQLSession<?> session, final Object dbObject, final NoSQLDescriptionField descriptionField, final Object entity,
                             final NoSQLEntityCache cache, final AbstractNoSQLObjectMapper mapper) {
        mapper.getConverters().fromDocument(dbObject, descriptionField, entity);
    }

    @Override
    public void toDocument(final Object entity, final NoSQLDescriptionField descriptionField, final Object dbObject, final Map<Object, Object> involvedObjects,
                           final AbstractNoSQLObjectMapper mapper) {
        try {
            mapper.getConverters().toDocument(entity, descriptionField, dbObject, mapper.getOptions());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
