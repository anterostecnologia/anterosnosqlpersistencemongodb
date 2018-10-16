package br.com.anteros.nosql.persistence.mongodb.geo;

import com.mongodb.BasicDBObject;

import br.com.anteros.nosql.persistence.converters.NoSQLSimpleValueConverter;
import br.com.anteros.nosql.persistence.converters.NoSQLTypeConverter;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;
import br.com.anteros.nosql.persistence.session.mapping.AbstractNoSQLObjectMapper;

public class GeometryQueryConverter extends NoSQLTypeConverter implements NoSQLSimpleValueConverter {

 
    public GeometryQueryConverter(final AbstractNoSQLObjectMapper mapper) {
        super.setMapper(mapper);
    }

    @Override
    public Object decode(final Class<?> targetClass, final Object fromDBObject, final NoSQLDescriptionField descriptionField) {
        throw new UnsupportedOperationException("Should never have to decode a query object");
    }

    @Override
    public Object encode(final Object value, final NoSQLDescriptionField descriptionField) {
        Object encode = getMapper().getConverters().encode(((Geometry) value));
        return new BasicDBObject("$geometry", encode);
    }
}
