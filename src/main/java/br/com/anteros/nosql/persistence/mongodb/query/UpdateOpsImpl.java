package br.com.anteros.nosql.persistence.mongodb.query;

import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.querydsl.core.QueryException;

import br.com.anteros.nosql.persistence.metadata.NoSQLDescriptionField;
import br.com.anteros.nosql.persistence.mongodb.mapping.MongoObjectMapper;
import br.com.anteros.nosql.persistence.session.mapping.AbstractNoSQLObjectMapper;

public class UpdateOpsImpl<T> implements UpdateOperations<T> {
	private final AbstractNoSQLObjectMapper mapper;
	private final Class<T> clazz;
	private Map<String, Map<String, Object>> ops = new HashMap<String, Map<String, Object>>();
	private boolean validateNames = true;
	private boolean isolated;

	public UpdateOpsImpl(final Class<T> type, final AbstractNoSQLObjectMapper mapper) {
		this.mapper = mapper;
		clazz = type;
	}

	@Override
	public UpdateOperations<T> addToSet(final String field, final Object value) {
		if (value == null) {
			throw new QueryException("Value cannot be null.");
		}

		add(UpdateOperator.ADD_TO_SET, field, value, true);
		return this;
	}

	@Override
	public UpdateOperations<T> addToSet(final String field, final List<?> values) {
		if (values == null || values.isEmpty()) {
			throw new QueryException("Values cannot be null or empty.");
		}

		add(UpdateOperator.ADD_TO_SET_EACH, field, values, true);
		return this;
	}

	@Override
	public UpdateOperations<T> push(final String field, final Object value) {
		return push(field, value instanceof List ? (List<?>) value : singletonList(value), new PushOptions());
	}

	@Override
	public UpdateOperations<T> push(final String field, final Object value, final PushOptions options) {
		return push(field, value instanceof List ? (List<?>) value : singletonList(value), options);
	}

	@Override
	public UpdateOperations<T> push(final String field, final List<?> values) {
		return push(field, values, new PushOptions());
	}

	@Override
	public UpdateOperations<T> push(final String field, final List<?> values, final PushOptions options) {
		if (values == null || values.isEmpty()) {
			throw new QueryException("Values cannot be null or empty.");
		}

		PathTarget pathTarget = new PathTarget(mapper, mapper.getDescriptionEntityManager().getDescriptionEntity(clazz),
				field);
		if (!validateNames) {
			pathTarget.disableValidation();
		}

		BasicDBObject dbObject = new BasicDBObject(UpdateOperator.EACH.val(),
				((MongoObjectMapper) mapper).toMongoObject(pathTarget.getTarget(), null, values));
		options.update(dbObject);
		addOperation(UpdateOperator.PUSH, pathTarget.translatedPath(), dbObject);

		return this;
	}

	@Override
	public UpdateOperations<T> dec(final String field) {
		return inc(field, -1);
	}

	@Override
	public UpdateOperations<T> dec(final String field, final Number value) {
		if ((value instanceof Long) || (value instanceof Integer)) {
			return inc(field, (value.longValue() * -1));
		}
		if ((value instanceof Double) || (value instanceof Float)) {
			return inc(field, (value.doubleValue() * -1));
		}
		throw new IllegalArgumentException(
				"Currently only the following types are allowed: integer, long, double, float.");
	}

	@Override
	public UpdateOperations<T> disableValidation() {
		validateNames = false;
		return this;
	}

	@Override
	public UpdateOperations<T> enableValidation() {
		validateNames = true;
		return this;
	}

	@Override
	public UpdateOperations<T> inc(final String field) {
		return inc(field, 1);
	}

	@Override
	public UpdateOperations<T> inc(final String field, final Number value) {
		if (value == null) {
			throw new QueryException("Value cannot be null.");
		}
		add(UpdateOperator.INC, field, value, false);
		return this;
	}

	@Override
	public UpdateOperations<T> isolated() {
		isolated = true;
		return this;
	}

	@Override
	public UpdateOperations<T> max(final String field, final Number value) {
		add(UpdateOperator.MAX, field, value, false);
		return this;
	}

	@Override
	public UpdateOperations<T> min(final String field, final Number value) {
		add(UpdateOperator.MIN, field, value, false);
		return this;
	}

	@Override
	public UpdateOperations<T> removeAll(final String field, final Object value) {
		if (value == null) {
			throw new QueryException("Value cannot be null.");
		}
		add(UpdateOperator.PULL, field, value, true);
		return this;
	}

	@Override
	public UpdateOperations<T> removeAll(final String field, final List<?> values) {
		if (values == null || values.isEmpty()) {
			throw new QueryException("Value cannot be null or empty.");
		}

		add(UpdateOperator.PULL_ALL, field, values, true);
		return this;
	}

	@Override
	public UpdateOperations<T> removeFirst(final String field) {
		return remove(field, true);
	}

	@Override
	public UpdateOperations<T> removeLast(final String field) {
		return remove(field, false);
	}

	@Override
	public UpdateOperations<T> set(final String field, final Object value) {
		if (value == null) {
			throw new QueryException("Value cannot be null.");
		}

		add(UpdateOperator.SET, field, value, true);
		return this;
	}

	@Override
	public UpdateOperations<T> setOnInsert(final String field, final Object value) {
		if (value == null) {
			throw new QueryException("Value cannot be null.");
		}

		add(UpdateOperator.SET_ON_INSERT, field, value, true);
		return this;
	}

	@Override
	public UpdateOperations<T> unset(final String field) {
		add(UpdateOperator.UNSET, field, 1, false);
		return this;
	}

	public DBObject getOps() {
		return new BasicDBObject(ops);
	}

	@SuppressWarnings("unchecked")
	public void setOps(final DBObject ops) {
		this.ops = (Map<String, Map<String, Object>>) ops;
	}

	@Override
	public boolean isIsolated() {
		return isolated;
	}

	protected void add(final UpdateOperator op, final String f, final Object value, final boolean convert) {
		if (value == null) {
			throw new QueryException("Val cannot be null");
		}

		Object val = value;
		PathTarget pathTarget = new PathTarget(mapper, mapper.getDescriptionEntityManager().getDescriptionEntity(clazz),
				f);
		if (!validateNames) {
			pathTarget.disableValidation();
		}
		NoSQLDescriptionField descriptionField = pathTarget.getTarget();

		if (convert) {
			if (UpdateOperator.PULL_ALL.equals(op) && value instanceof List) {
				val = toDBObjList(descriptionField, (List<?>) value);
			} else {
				val = ((MongoObjectMapper) mapper).toMongoObject(descriptionField, null, value);
			}
		}

		if (UpdateOperator.ADD_TO_SET_EACH.equals(op)) {
			val = new BasicDBObject(UpdateOperator.EACH.val(), val);
		}

		addOperation(op, pathTarget.translatedPath(), val);
	}

	private void addOperation(final UpdateOperator op, final String fieldName, final Object val) {
		final String opString = op.val();

		if (!ops.containsKey(opString)) {
			ops.put(opString, new LinkedHashMap<String, Object>());
		}
		ops.get(opString).put(fieldName, val);
	}

	protected UpdateOperations<T> remove(final String fieldExpr, final boolean firstNotLast) {
		add(UpdateOperator.POP, fieldExpr, (firstNotLast) ? -1 : 1, false);
		return this;
	}

	protected List<Object> toDBObjList(final NoSQLDescriptionField mf, final List<?> values) {
		final List<Object> list = new ArrayList<Object>(values.size());
		for (final Object obj : values) {
			list.add(((MongoObjectMapper) mapper).toMongoObject(mf, null, obj));
		}

		return list;
	}

}
