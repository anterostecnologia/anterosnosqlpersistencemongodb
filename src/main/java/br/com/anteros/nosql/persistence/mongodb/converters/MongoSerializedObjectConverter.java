package br.com.anteros.nosql.persistence.mongodb.converters;


import static java.lang.String.format;

import java.io.IOException;

import org.bson.types.Binary;

import br.com.anteros.nosql.persistence.converters.NoSQLMappingException;
import br.com.anteros.nosql.persistence.converters.NoSQLTypeConverter;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;
import br.com.anteros.nosql.persistence.metadata.annotations.Serialized;


public class MongoSerializedObjectConverter extends NoSQLTypeConverter {
    @Override
    public Object decode(final Class<?> targetClass, final Object fromDBObject, final NoSQLDescriptionField descriptionField) {
        if (fromDBObject == null) {
            return null;
        }

        if (!((fromDBObject instanceof Binary) || (fromDBObject instanceof byte[]))) {
            throw new NoSQLMappingException(format("The stored data is not a DBBinary or byte[] instance for %s ; it is a %s",
            		descriptionField.getFullName(), fromDBObject.getClass().getName()));
        }

        try {
            final boolean useCompression = !descriptionField.isDisableCompression();
            return NoSQLSerializer.deserialize(fromDBObject, useCompression);
        } catch (IOException e) {
            throw new NoSQLMappingException("While deserializing to " + descriptionField.getFullName(), e);
        } catch (ClassNotFoundException e) {
            throw new NoSQLMappingException("While deserializing to " + descriptionField.getFullName(), e);
        }
    }

    @Override
    public Object encode(final Object value, final NoSQLDescriptionField descriptionField) {
        if (value == null) {
            return null;
        }
        try {
            final boolean useCompression = !descriptionField.isDisableCompression();
            return NoSQLSerializer.serialize(value, useCompression);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected boolean isSupported(final Class<?> c, final NoSQLDescriptionField descriptionField) {
        return descriptionField != null && (descriptionField.isSerialized());
    }

}
