package br.com.anteros.nosql.persistence.mongodb.converters;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.bson.types.Decimal128;

import br.com.anteros.nosql.persistence.converters.NoSQLSimpleValueConverter;
import br.com.anteros.nosql.persistence.converters.NoSQLTypeConverter;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;

public class MongoBigDecimalConverter extends NoSQLTypeConverter implements NoSQLSimpleValueConverter {

	public MongoBigDecimalConverter() {
		super(BigDecimal.class);
	}

	@Override
	public Object decode(final Class<?> targetClass, final Object value, final NoSQLDescriptionField descriptionField) {
		if (value == null) {
			return null;
		}

		if (value instanceof BigDecimal) {
			return value;
		}

		if (value instanceof Decimal128) {
			return ((Decimal128) value).bigDecimalValue();
		}

		if (value instanceof BigInteger) {
			return new BigDecimal(((BigInteger) value));
		}

		if (value instanceof Double) {
			return new BigDecimal(((Double) value));
		}

		if (value instanceof Long) {
			return new BigDecimal(((Long) value));
		}

		if (value instanceof Number) {
			return new BigDecimal(((Number) value).doubleValue());
		}

		return new BigDecimal(value.toString());
	}

	@Override
	public Object encode(final Object value, final NoSQLDescriptionField descriptionField) {
		if (value instanceof BigDecimal) {
			return new Decimal128((BigDecimal) value);
		}
		return super.encode(value, descriptionField);
	}
}
