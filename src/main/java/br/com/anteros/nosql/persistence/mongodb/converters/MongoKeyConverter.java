package br.com.anteros.nosql.persistence.mongodb.converters;

import com.mongodb.DBRef;

import br.com.anteros.nosql.persistence.converters.ConverterException;
import br.com.anteros.nosql.persistence.converters.Key;
import br.com.anteros.nosql.persistence.converters.NoSQLTypeConverter;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;
import br.com.anteros.nosql.persistence.mongodb.mapping.MongoObjectMapper;

public class MongoKeyConverter extends NoSQLTypeConverter {

	public MongoKeyConverter() {
		super(Key.class);
	}

	@Override
	public Object decode(final Class<?> targetClass, final Object o, final NoSQLDescriptionField descriptionField) {
		if (o == null) {
			return null;
		}
		if (!(o instanceof DBRef)) {
			throw new ConverterException(
					String.format("cannot convert %s to Key because it isn't a DBRef", o.toString()));
		}

		DBRef ref = (DBRef) o;

		final Class<?> keyType = descriptionField != null ? descriptionField.getConcreteType()
				: getMapper().getClassFromCollection(ref.getCollectionName());

		final Key<?> key = new Key<Object>(keyType, ref.getCollectionName(), ref.getId());

		return key;
	}


	@Override
	public Object encode(final Object t, final NoSQLDescriptionField descriptionField) {
		if (t == null) {
			return null;
		}
		return ((MongoObjectMapper)getMapper()).keyToDBRef((Key) t);
	}

}
