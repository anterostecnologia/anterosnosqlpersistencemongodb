package br.com.anteros.nosql.persistence.mongodb.converters;

import static java.lang.String.format;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.mongodb.DBObject;

import br.com.anteros.core.utils.ListUtils;
import br.com.anteros.core.utils.ReflectionUtils;
import br.com.anteros.nosql.persistence.converters.ConverterException;
import br.com.anteros.nosql.persistence.converters.NoSQLConverters;
import br.com.anteros.nosql.persistence.converters.NoSQLTypeConverter;
import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;
import br.com.anteros.nosql.persistence.session.mapping.NoSQLObjectFactory;

public class MongoIterableConverter extends NoSQLTypeConverter {

	public Object decode(final Class<?> targetClass, final Object fromNoSQLObject,
			final NoSQLDescriptionField descriptionField) {
		if (descriptionField == null || fromNoSQLObject == null) {
			return fromNoSQLObject;
		}

		final Class<?> subtypeDest = descriptionField.getSubClass();
		final Collection values = createNewCollection(descriptionField);

		final NoSQLConverters converters = getMapper().getConverters();
		if (fromNoSQLObject.getClass().isArray()) {
			for (final Object o : (Object[]) fromNoSQLObject) {
				values.add(converters.decode((subtypeDest != null) ? subtypeDest : o.getClass(), o, descriptionField));
			}
		} else if (fromNoSQLObject instanceof Iterable) {
			// map back to the java data type
			// (List/Set/Array[])
			for (final Object o : (Iterable) fromNoSQLObject) {
				if (o instanceof DBObject) {
					values.add(
							converters.decode((subtypeDest != null) ? subtypeDest : o.getClass(), o, descriptionField));
				} else {
					values.add(
							converters.decode((subtypeDest != null) ? subtypeDest : o.getClass(), o, descriptionField));
				}
			}
		} else {
			// Single value case.
			values.add(converters.decode((subtypeDest != null) ? subtypeDest : fromNoSQLObject.getClass(),
					fromNoSQLObject, descriptionField));
		}

		if (descriptionField.isArray()) {
			return ListUtils.convertToArray(subtypeDest, (List) values);
		} else {
			return values;
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public Object encode(final Object value, final NoSQLDescriptionField descriptionField) {

		if (value == null) {
			return null;
		}

		final Iterable<?> iterableValues;

		if (value.getClass().isArray()) {
			if (Array.getLength(value) == 0 || value.getClass().getComponentType().isPrimitive()) {
				return value;
			}

			iterableValues = Arrays.asList((Object[]) value);
		} else {
			if (!(value instanceof Iterable)) {
				throw new ConverterException(
						format("Cannot cast %s to Iterable for MappedField: %s", value.getClass(), descriptionField));
			}
			iterableValues = (Iterable<?>) value;
		}

		final List values = new ArrayList();
		if (descriptionField != null) {
			for (final Object o : iterableValues) {
				values.add(getMapper().getConverters().encode(descriptionField.getSubClass(), o));
			}
		} else {
			for (final Object o : iterableValues) {
				values.add(getMapper().getConverters().encode(o));
			}
		}
		return !values.isEmpty() || getMapper().getOptions().isStoreEmpties() ? values : null;
	}

	@Override
	protected boolean isSupported(final Class<?> c, final NoSQLDescriptionField descriptionField) {
		if (descriptionField != null) {
			return descriptionField.isAnyArrayOrCollection() && !descriptionField.isAnyMap();
		} else {
			return c.isArray() || ReflectionUtils.isImplementsInterface(c, Iterable.class);
		}
	}

	private Collection<?> createNewCollection(final NoSQLDescriptionField descriptionField) {
		final NoSQLObjectFactory of = getMapper().getOptions().getObjectFactory();
		return descriptionField.isAnyArrayOrCollection() ? of.createSet(descriptionField) : of.createList(descriptionField);
	}
}
