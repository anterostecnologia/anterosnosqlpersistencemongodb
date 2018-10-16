package br.com.anteros.nosql.persistence.mongodb.converters;

import java.util.Optional;

import br.com.anteros.nosql.persistence.converters.NoSQLTypeConverter;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;

public class OptionalConverter extends NoSQLTypeConverter {

	private MongoDefaultConverters defaultConverters;

	public OptionalConverter(MongoDefaultConverters defaultConverters) {
		super(Optional.class);
		this.defaultConverters = defaultConverters;
	}

	@Override
	public Object encode(Object value, NoSQLDescriptionField descriptionField) {
		if (value == null) {
			return null;
		}

		Optional optional = (Optional) value;
		return optional.map(defaultConverters::encode).orElse(null);
	}

	@Override
	public Object decode(Class type, Object fromDbObject, NoSQLDescriptionField descriptionField) {
		return Optional.ofNullable(fromDbObject);
	}
}