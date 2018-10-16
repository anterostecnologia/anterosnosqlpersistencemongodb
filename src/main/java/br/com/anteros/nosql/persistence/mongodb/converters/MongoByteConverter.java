package br.com.anteros.nosql.persistence.mongodb.converters;

import org.bson.types.Binary;

import br.com.anteros.nosql.persistence.converters.ByteConverter;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;

public class MongoByteConverter extends ByteConverter{

	
	@Override
	public Object decode(Class<?> targetClass, Object val, NoSQLDescriptionField descriptionField) {
		if (val instanceof Binary) {
			return ((Binary)val).getData();
		}
		return super.decode(targetClass, val, descriptionField);
	}
}
