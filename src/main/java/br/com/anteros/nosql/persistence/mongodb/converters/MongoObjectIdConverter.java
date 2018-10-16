package br.com.anteros.nosql.persistence.mongodb.converters;

import org.bson.types.ObjectId;

import br.com.anteros.nosql.persistence.converters.NoSQLSimpleValueConverter;
import br.com.anteros.nosql.persistence.converters.NoSQLTypeConverter;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;

public class MongoObjectIdConverter extends NoSQLTypeConverter implements NoSQLSimpleValueConverter {

	public MongoObjectIdConverter() {
		super(ObjectId.class);
	}

	@Override
	public Object decode(final Class<?> targetClass, final Object val, final NoSQLDescriptionField descriptionField) {
		if (val == null) {
			return null;
		}

		if (val instanceof ObjectId) {
			return val;
		}

		return new ObjectId(val.toString());
	}
}
