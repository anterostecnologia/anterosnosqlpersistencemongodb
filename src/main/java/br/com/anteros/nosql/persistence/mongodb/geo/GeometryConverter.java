package br.com.anteros.nosql.persistence.mongodb.geo;

import com.mongodb.DBObject;

import br.com.anteros.nosql.persistence.converters.NoSQLSimpleValueConverter;
import br.com.anteros.nosql.persistence.converters.NoSQLTypeConverter;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;


public class GeometryConverter extends NoSQLTypeConverter implements NoSQLSimpleValueConverter {
  
    public GeometryConverter() {
        super(Geometry.class);
    }

    @Override
    public Object decode(final Class<?> targetClass, final Object fromDBObject, final NoSQLDescriptionField descriptionField) {
        DBObject dbObject = (DBObject) fromDBObject;
        String type = (String) dbObject.get("type");
        return getMapper().getConverters().decode(GeoJsonType.fromString(type).getTypeClass(), fromDBObject, descriptionField);
    }
}
