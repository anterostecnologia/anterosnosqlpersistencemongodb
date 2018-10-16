package br.com.anteros.nosql.persistence.mongodb.geo;

import com.mongodb.BasicDBObject;

import br.com.anteros.nosql.persistence.converters.NoSQLSimpleValueConverter;
import br.com.anteros.nosql.persistence.converters.NoSQLTypeConverter;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;



public class NamedCoordinateReferenceSystemConverter extends NoSQLTypeConverter implements NoSQLSimpleValueConverter {
   
    public NamedCoordinateReferenceSystemConverter() {
        super(NamedCoordinateReferenceSystem.class);
    }

    @Override
    public Object decode(final Class<?> targetClass, final Object fromDBObject, final NoSQLDescriptionField descriptionField) {
        throw new UnsupportedOperationException("We should never need to decode these");
    }

    @Override
    public Object encode(final Object value, final NoSQLDescriptionField descriptionField) {
        NamedCoordinateReferenceSystem crs = (NamedCoordinateReferenceSystem) value;
        final BasicDBObject dbObject = new BasicDBObject("type", crs.getType().getTypeName());
        dbObject.put("properties", new BasicDBObject("name", crs.getName()));

        return dbObject;
    }

    @Override
    protected boolean isSupported(final Class<?> c, final NoSQLDescriptionField descriptionField) {
        return CoordinateReferenceSystem.class.isAssignableFrom(c);
    }
}
