package br.com.anteros.nosql.persistence.mongodb.dialect;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.Date;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Pattern;

import org.bson.Document;
import org.bson.types.CodeWScope;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;

import br.com.anteros.nosql.persistence.client.NoSQLDataSourceBuilder;
import br.com.anteros.nosql.persistence.client.NoSQLSessionBuilder;
import br.com.anteros.nosql.persistence.converters.Key;
import br.com.anteros.nosql.persistence.converters.NoSQLConverters;
import br.com.anteros.nosql.persistence.dialect.NoSQLDialect;
import br.com.anteros.nosql.persistence.mongodb.client.MongoDataSourceBuilder;
import br.com.anteros.nosql.persistence.mongodb.client.MongoSessionBuilder;
import br.com.anteros.nosql.persistence.mongodb.converters.MongoDefaultConverters;
import br.com.anteros.nosql.persistence.mongodb.session.MongoSession;
import br.com.anteros.nosql.persistence.mongodb.transaction.MongoTransactionFactory;
import br.com.anteros.nosql.persistence.session.mapping.AbstractNoSQLObjectMapper;
import br.com.anteros.nosql.persistence.session.transaction.NoSQLTransactionFactory;

public class MongoDialect extends NoSQLDialect {

	private MongoTransactionFactory transactionFactory = new MongoTransactionFactory();

	@Override
	public NoSQLTransactionFactory getTransactionFactory() {
		return transactionFactory;
	}

	@Override
	public NoSQLDataSourceBuilder getDataSourceBuilder() throws Exception {
		return new MongoDataSourceBuilder();
	}

	@Override
	public NoSQLSessionBuilder getSessionBuilder() throws Exception {
		return new MongoSessionBuilder();
	}

	@Override
	public NoSQLConverters getDefaultConverters(AbstractNoSQLObjectMapper mapper) {
		return new MongoDefaultConverters(mapper);
	}

	@Override
	public Object getDbObjectValue(String fieldName, Object noSQLObject, boolean isIdentifier) {
		if (isIdentifier)
			return ((Document) noSQLObject).get("_id");

		return ((Document) noSQLObject).get(fieldName);
	}

	@Override
	public void setDbObjectValue(Object noSQLObject, String name, Object value, boolean isIdentifier) {
		if (isIdentifier) {
			if (value instanceof ObjectId) {
				((Document) noSQLObject).put(name, value);
			} else if (value instanceof String) {
				((Document) noSQLObject).put(name, new ObjectId(value.toString()));
			}
		} else {
			((Document) noSQLObject).put(name, value);
		}
	}

	@Override
	public boolean isDatabaseType(Class<?> type) {
		return isPropertyType(type);
	}

	@Override
	public boolean isDatabaseType(Type type) {
		return isPropertyType(type);
	}

	public static boolean isPropertyType(final Class<?> type) {
		return type != null && (isPrimitiveLike(type) || type == DBRef.class || type == Pattern.class
				|| type == CodeWScope.class || type == ObjectId.class || type == Key.class || type == DBObject.class
				|| type == BasicDBObject.class || type == Document.class);

	}

	public static boolean isPrimitiveLike(final Class<?> type) {
		return type != null && (type == String.class || type == char.class || type == Character.class
				|| type == short.class || type == Short.class || type == Integer.class || type == int.class
				|| type == Long.class || type == long.class || type == Double.class || type == double.class
				|| type == float.class || type == Float.class || type == Boolean.class || type == boolean.class
				|| type == Byte.class || type == byte.class || type == Date.class || type == Locale.class
				|| type == Class.class || type == UUID.class || type == URI.class || type.isEnum());

	}

	public static boolean isPropertyType(final Type type) {
		if (type instanceof GenericArrayType) {
			return isPropertyType(((GenericArrayType) type).getGenericComponentType());
		}
		if (type instanceof ParameterizedType) {
			return isPropertyType(((ParameterizedType) type).getRawType());
		}
		return type instanceof Class && isPropertyType((Class) type);
	}

	@Override
	public Class<?> getSessionInterface() {
		return MongoSession.class;
	}

}
