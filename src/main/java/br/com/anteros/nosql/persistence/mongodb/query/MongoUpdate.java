/*
 * Copyright 2010-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package br.com.anteros.nosql.persistence.mongodb.query;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.bson.Document;

import br.com.anteros.core.utils.Assert;
import br.com.anteros.core.utils.ObjectUtils;
import br.com.anteros.core.utils.StringUtils;
import br.com.anteros.nosql.persistence.session.query.NoSQLUpdate;
import br.com.anteros.nosql.persistence.session.query.Sort;
import br.com.anteros.nosql.persistence.session.query.Sort.Order;

public class MongoUpdate implements NoSQLUpdate<Document> {

	public enum Position {
		LAST, FIRST
	}

	private boolean isolated = false;
	private Set<String> keysToUpdate = new HashSet<>();
	private Map<String, Object> modifierOps = new LinkedHashMap<>();
	private Map<String, PushOperatorBuilder> pushCommandBuilders = new LinkedHashMap<>(1);

	public static MongoUpdate update(String key, Object value) {
		return new MongoUpdate().set(key, value);
	}

	public static MongoUpdate fromDocument(Document object, String... exclude) {

		MongoUpdate update = new MongoUpdate();
		List<String> excludeList = Arrays.asList(exclude);

		for (String key : object.keySet()) {

			if (excludeList.contains(key)) {
				continue;
			}

			Object value = object.get(key);
			update.modifierOps.put(key, value);
			if (isKeyword(key) && value instanceof Document) {
				update.keysToUpdate.addAll(((Document) value).keySet());
			} else {
				update.keysToUpdate.add(key);
			}
		}

		return update;
	}
	
	

	public MongoUpdate set(String key, Object value) {
		addMultiFieldOperation("$set", key, value);
		return this;
	}

	public MongoUpdate setOnInsert(String key, Object value) {
		addMultiFieldOperation("$setOnInsert", key, value);
		return this;
	}

	public MongoUpdate unset(String key) {
		addMultiFieldOperation("$unset", key, 1);
		return this;
	}

	public MongoUpdate inc(String key, Number inc) {
		addMultiFieldOperation("$inc", key, inc);
		return this;
	}

	public MongoUpdate push(String key, Object value) {
		addMultiFieldOperation("$push", key, value);
		return this;
	}

	public PushOperatorBuilder push(String key) {

		if (!pushCommandBuilders.containsKey(key)) {
			pushCommandBuilders.put(key, new PushOperatorBuilder(key));
		}
		return pushCommandBuilders.get(key);
	}

	public AddToSetBuilder addToSet(String key) {
		return new AddToSetBuilder(key);
	}

	public MongoUpdate addToSet(String key, Object value) {
		addMultiFieldOperation("$addToSet", key, value);
		return this;
	}

	public MongoUpdate pop(String key, Position pos) {
		addMultiFieldOperation("$pop", key, pos == Position.FIRST ? -1 : 1);
		return this;
	}

	public MongoUpdate pull(String key, Object value) {
		addMultiFieldOperation("$pull", key, value);
		return this;
	}

	public MongoUpdate pullAll(String key, Object[] values) {
		addMultiFieldOperation("$pullAll", key, Arrays.asList(values));
		return this;
	}

	public MongoUpdate rename(String oldName, String newName) {
		addMultiFieldOperation("$rename", oldName, newName);
		return this;
	}

	public MongoUpdate currentDate(String key) {
		addMultiFieldOperation("$currentDate", key, true);
		return this;
	}

	public MongoUpdate currentTimestamp(String key) {
		addMultiFieldOperation("$currentDate", key, new Document("$type", "timestamp"));
		return this;
	}

	public MongoUpdate multiply(String key, Number multiplier) {
		Assert.notNull(multiplier, "Multiplier must not be null.");
		addMultiFieldOperation("$mul", key, multiplier.doubleValue());
		return this;
	}

	public MongoUpdate max(String key, Object value) {
		Assert.notNull(value, "Value for max operation must not be null.");
		addMultiFieldOperation("$max", key, value);
		return this;
	}

	public MongoUpdate min(String key, Object value) {

		Assert.notNull(value, "Value for min operation must not be null.");
		addMultiFieldOperation("$min", key, value);
		return this;
	}

	public BitwiseOperatorBuilder bitwise(String key) {
		return new BitwiseOperatorBuilder(this, key);
	}

	public MongoUpdate isolated() {

		isolated = true;
		return this;
	}

	public Boolean isIsolated() {
		return isolated;
	}

	public Document getUpdateObject() {
		return new Document(modifierOps);
	}

	protected void addMultiFieldOperation(String operator, String key, Object value) {

		Assert.hasText(key, "Key/Path for update must not be null or blank.");
		Object existingValue = this.modifierOps.get(operator);
		Document keyValueMap;

		if (existingValue == null) {
			keyValueMap = new Document();
			this.modifierOps.put(operator, keyValueMap);
		} else {
			if (existingValue instanceof Document) {
				keyValueMap = (Document) existingValue;
			} else {
				throw new InvalidDataAccessApiUsageException(
						"Modifier Operations should be a LinkedHashMap but was " + existingValue.getClass());
			}
		}

		keyValueMap.put(key, value);
		this.keysToUpdate.add(key);
	}

	public boolean modifies(String key) {
		return this.keysToUpdate.contains(key);
	}

	private static boolean isKeyword(String key) {
		return StringUtils.startsWithIgnoreCase(key, "$");
	}

	@Override
	public int hashCode() {
		return Objects.hash(getUpdateObject(), isolated);
	}

	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}

		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}

		MongoUpdate that = (MongoUpdate) obj;
		if (this.isolated != that.isolated) {
			return false;
		}

		return Objects.equals(this.getUpdateObject(), that.getUpdateObject());
	}

	@Override
	public String toString() {

		Document doc = getUpdateObject();

		if (isIsolated()) {
			doc.append("$isolated", 1);
		}

		return SerializationUtils.serializeToJsonSafely(doc);
	}

	public static class Modifiers {

		private Map<String, Modifier> modifiers;

		public Modifiers() {
			this.modifiers = new LinkedHashMap<>(1);
		}

		public Collection<Modifier> getModifiers() {
			return Collections.unmodifiableCollection(this.modifiers.values());
		}

		public void addModifier(Modifier modifier) {
			this.modifiers.put(modifier.getKey(), modifier);
		}

		public boolean isEmpty() {
			return modifiers.isEmpty();
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(modifiers);
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (obj == null || getClass() != obj.getClass()) {
				return false;
			}

			Modifiers that = (Modifiers) obj;
			return Objects.equals(this.modifiers, that.modifiers);
		}

		@Override
		public String toString() {
			return SerializationUtils.serializeToJsonSafely(this.modifiers);
		}
	}

	public interface Modifier {

		String getKey();

		Object getValue();

		default String toJsonString() {
			return SerializationUtils.serializeToJsonSafely(Collections.singletonMap(getKey(), getValue()));
		}
	}

	private static abstract class AbstractModifier implements Modifier {

		@Override
		public int hashCode() {
			return ObjectUtils.nullSafeHashCode(getKey()) + ObjectUtils.nullSafeHashCode(getValue());
		}

		@Override
		public boolean equals(Object that) {

			if (this == that) {
				return true;
			}

			if (that == null || getClass() != that.getClass()) {
				return false;
			}

			if (!Objects.equals(getKey(), ((Modifier) that).getKey())) {
				return false;
			}

			return Objects.deepEquals(getValue(), ((Modifier) that).getValue());
		}

		@Override
		public String toString() {
			return toJsonString();
		}
	}

	private static class Each extends AbstractModifier {

		private Object[] values;

		Each(Object... values) {
			this.values = extractValues(values);
		}

		private Object[] extractValues(Object[] values) {

			if (values == null || values.length == 0) {
				return values;
			}

			if (values.length == 1 && values[0] instanceof Collection) {
				return ((Collection<?>) values[0]).toArray();
			}

			return Arrays.copyOf(values, values.length);
		}

		@Override
		public String getKey() {
			return "$each";
		}

		@Override
		public Object getValue() {
			return this.values;
		}
	}

	private static class PositionModifier extends AbstractModifier {

		private final int position;

		PositionModifier(int position) {
			this.position = position;
		}

		@Override
		public String getKey() {
			return "$position";
		}

		@Override
		public Object getValue() {
			return position;
		}
	}

	private static class Slice extends AbstractModifier {

		private int count;

		Slice(int count) {
			this.count = count;
		}

		@Override
		public String getKey() {
			return "$slice";
		}

		@Override
		public Object getValue() {
			return this.count;
		}
	}

	private static class SortModifier extends AbstractModifier {

		private final Object sort;

		SortModifier(Sort.Direction direction) {

			Assert.notNull(direction, "Direction must not be null!");
			this.sort = direction.isAscending() ? 1 : -1;
		}

		SortModifier(Sort sort) {

			Assert.notNull(sort, "Sort must not be null!");

			while (sort.iterator().hasNext()) {
				Order order = sort.iterator().next();

				if (order.isIgnoreCase()) {
					throw new IllegalArgumentException(String.format(
							"Given sort contained an Order for %s with ignore case! "
									+ "MongoDB does not support sorting ignoring case currently!",
							order.getProperty()));
				}
			}

			this.sort = sort;
		}

		@Override
		public String getKey() {
			return "$sort";
		}

		@Override
		public Object getValue() {
			return this.sort;
		}
	}

	public class PushOperatorBuilder {

		private final String key;
		private final Modifiers modifiers;

		PushOperatorBuilder(String key) {
			this.key = key;
			this.modifiers = new Modifiers();
		}

		public MongoUpdate each(Object... values) {

			this.modifiers.addModifier(new Each(values));
			return MongoUpdate.this.push(key, this.modifiers);
		}

		public PushOperatorBuilder slice(int count) {

			this.modifiers.addModifier(new Slice(count));
			return this;
		}

		public PushOperatorBuilder sort(Sort.Direction direction) {

			Assert.notNull(direction, "Direction must not be null.");
			this.modifiers.addModifier(new SortModifier(direction));
			return this;
		}

		public PushOperatorBuilder sort(Sort sort) {

			Assert.notNull(sort, "Sort must not be null.");
			this.modifiers.addModifier(new SortModifier(sort));
			return this;
		}

		public PushOperatorBuilder atPosition(int position) {

			if (position < 0) {
				throw new IllegalArgumentException("Position must be greater than or equal to zero.");
			}

			this.modifiers.addModifier(new PositionModifier(position));

			return this;
		}

		public PushOperatorBuilder atPosition(Position position) {

			if (position == null || Position.LAST.equals(position)) {
				return this;
			}

			this.modifiers.addModifier(new PositionModifier(0));

			return this;
		}

		public MongoUpdate value(Object value) {

			if (this.modifiers.isEmpty()) {
				return MongoUpdate.this.push(key, value);
			}

			this.modifiers.addModifier(new Each(Collections.singletonList(value)));
			return MongoUpdate.this.push(key, this.modifiers);
		}

		@Override
		public int hashCode() {
			return Objects.hash(getOuterType(), key, modifiers);
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (obj == null || getClass() != obj.getClass()) {
				return false;
			}

			PushOperatorBuilder that = (PushOperatorBuilder) obj;

			if (!Objects.equals(getOuterType(), that.getOuterType())) {
				return false;
			}

			return Objects.equals(this.key, that.key) && Objects.equals(this.modifiers, that.modifiers);
		}

		private MongoUpdate getOuterType() {
			return MongoUpdate.this;
		}
	}

	public class AddToSetBuilder {

		private final String key;

		public AddToSetBuilder(String key) {
			this.key = key;
		}

		public MongoUpdate each(Object... values) {
			return MongoUpdate.this.addToSet(this.key, new Each(values));
		}

		public MongoUpdate value(Object value) {
			return MongoUpdate.this.addToSet(this.key, value);
		}
	}

	public static class BitwiseOperatorBuilder {

		private final String key;
		private final MongoUpdate reference;
		private static final String BIT_OPERATOR = "$bit";

		private enum BitwiseOperator {
			AND, OR, XOR;

			@Override
			public String toString() {
				return super.toString().toLowerCase();
			};
		}

		protected BitwiseOperatorBuilder(MongoUpdate reference, String key) {

			Assert.notNull(reference, "Reference must not be null!");
			Assert.notNull(key, "Key must not be null!");

			this.reference = reference;
			this.key = key;
		}

		public MongoUpdate and(long value) {

			addFieldOperation(BitwiseOperator.AND, value);
			return reference;
		}

		public MongoUpdate or(long value) {

			addFieldOperation(BitwiseOperator.OR, value);
			return reference;
		}

		public MongoUpdate xor(long value) {

			addFieldOperation(BitwiseOperator.XOR, value);
			return reference;
		}

		private void addFieldOperation(BitwiseOperator operator, Number value) {
			reference.addMultiFieldOperation(BIT_OPERATOR, key, new Document(operator.toString(), value));
		}
	}
}
