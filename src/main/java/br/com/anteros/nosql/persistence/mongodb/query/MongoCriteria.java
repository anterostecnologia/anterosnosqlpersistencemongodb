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

import static br.com.anteros.core.utils.ObjectUtils.nullSafeHashCode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.bson.BSON;
import org.bson.BsonRegularExpression;
import org.bson.Document;
import org.bson.types.Binary;

import com.mongodb.BasicDBList;

import br.com.anteros.core.utils.Assert;
import br.com.anteros.core.utils.Base64;
import br.com.anteros.core.utils.CollectionUtils;
import br.com.anteros.core.utils.ObjectUtils;
import br.com.anteros.core.utils.StringUtils;
import br.com.anteros.nosql.persistence.session.query.Example;
import br.com.anteros.nosql.persistence.session.query.NoSQLCriteria;


public class MongoCriteria implements CriteriaDefinition, NoSQLCriteria<Document> {


	private static final Object NOT_SET = new Object();

	private String key;
	private List<MongoCriteria> criteriaChain;
	private LinkedHashMap<String, Object> criteria = new LinkedHashMap<String, Object>();
	private Object isValue = NOT_SET;
	
	public static  MongoCriteria of() {
		return new MongoCriteria();
	}
	
	public static MongoCriteria where(String key) {
		return new MongoCriteria(key);
	}


	public static MongoCriteria byExample(Object example) {
		return byExample(Example.of(example));
	}

	public static MongoCriteria byExample(Example<?> example) {
		return new MongoCriteria().alike(example);
	}

	protected MongoCriteria() {
		this.criteriaChain = new ArrayList<MongoCriteria>();
	}

	protected MongoCriteria(String key) {
		this.criteriaChain = new ArrayList<MongoCriteria>();
		this.criteriaChain.add(this);
		this.key = key;
	}

	protected MongoCriteria(List<MongoCriteria> criteriaChain, String key) {
		this.criteriaChain = criteriaChain;
		this.criteriaChain.add(this);
		this.key = key;
	}

	public MongoCriteria and(String key) {
		return new MongoCriteria(this.criteriaChain, key);
	}

	public MongoCriteria is(Object o) {

		if (!isValue.equals(NOT_SET)) {
			throw new InvalidMongoDbApiUsageException(
					"Multiple 'is' values declared. You need to use 'and' with multiple criteria");
		}

		if (lastOperatorWasNot()) {
			throw new InvalidMongoDbApiUsageException("Invalid query: 'not' can't be used with 'is' - use 'ne' instead.");
		}

		this.isValue = o;
		return this;
	}

	private boolean lastOperatorWasNot() {
		return !this.criteria.isEmpty() && "$not".equals(this.criteria.keySet().toArray()[this.criteria.size() - 1]);
	}

	public MongoCriteria ne(Object o) {
		criteria.put("$ne", o);
		return this;
	}

	public MongoCriteria lt(Object o) {
		criteria.put("$lt", o);
		return this;
	}

	public MongoCriteria lte(Object o) {
		criteria.put("$lte", o);
		return this;
	}

	public MongoCriteria gt(Object o) {
		criteria.put("$gt", o);
		return this;
	}

	public MongoCriteria gte(Object o) {
		criteria.put("$gte", o);
		return this;
	}

	public MongoCriteria in(Object... o) {
		if (o.length > 1 && o[1] instanceof Collection) {
			throw new InvalidMongoDbApiUsageException(
					"You can only pass in one argument of type " + o[1].getClass().getName());
		}
		criteria.put("$in", Arrays.asList(o));
		return this;
	}

	public MongoCriteria in(Collection<?> c) {
		criteria.put("$in", c);
		return this;
	}

	public MongoCriteria nin(Object... o) {
		return nin(Arrays.asList(o));
	}

	public MongoCriteria nin(Collection<?> o) {
		criteria.put("$nin", o);
		return this;
	}

	public MongoCriteria mod(Number value, Number remainder) {
		List<Object> l = new ArrayList<Object>();
		l.add(value);
		l.add(remainder);
		criteria.put("$mod", l);
		return this;
	}

	public MongoCriteria all(Object... o) {
		return all(Arrays.asList(o));
	}

	public MongoCriteria all(Collection<?> o) {
		criteria.put("$all", o);
		return this;
	}

	public MongoCriteria size(int s) {
		criteria.put("$size", s);
		return this;
	}

	public MongoCriteria exists(boolean b) {
		criteria.put("$exists", b);
		return this;
	}

	public MongoCriteria type(int t) {
		criteria.put("$type", t);
		return this;
	}

	
	public MongoCriteria not() {
		return not(null);
	}

	private MongoCriteria not(Object value) {
		criteria.put("$not", value);
		return this;
	}

	public MongoCriteria regex(String re) {
		return regex(re, null);
	}

	public MongoCriteria regex(String re, String options) {
		return regex(toPattern(re, options));
	}

	public MongoCriteria regex(Pattern pattern) {

		Assert.notNull(pattern, "Pattern must not be null!");

		if (lastOperatorWasNot()) {
			return not(pattern);
		}

		this.isValue = pattern;
		return this;
	}

	public MongoCriteria regex(BsonRegularExpression regex) {

		if (lastOperatorWasNot()) {
			return not(regex);
		}

		this.isValue = regex;
		return this;
	}

	private Pattern toPattern(String regex, String options) {

		Assert.notNull(regex, "Regex string must not be null!");

		return Pattern.compile(regex, options == null ? 0 : BSON.regexFlags(options));
	}


	public MongoCriteria elemMatch(NoSQLCriteria<?> c) {
		criteria.put("$elemMatch", c.getCriteriaObject());
		return this;
	}

	public MongoCriteria alike(Example<?> sample) {

		criteria.put("$example", sample);
		this.criteriaChain.add(this);
		return this;
	}

	
	public BitwiseCriteriaOperators bits() {
		return new BitwiseCriteriaOperatorsImpl(this);
	}

	public MongoCriteria orOperator(NoSQLCriteria<?>... criteria) {
		BasicDBList bsonList = createCriteriaList(criteria);
		return registerCriteriaChainElement(new MongoCriteria("$or").is(bsonList));
	}

	public MongoCriteria norOperator(NoSQLCriteria<?>... criteria) {
		BasicDBList bsonList = createCriteriaList(criteria);
		return registerCriteriaChainElement(new MongoCriteria("$nor").is(bsonList));
	}

	public MongoCriteria andOperator(NoSQLCriteria<?>... criteria) {
		BasicDBList bsonList = createCriteriaList(criteria);
		return registerCriteriaChainElement(new MongoCriteria("$and").is(bsonList));
	}

	private MongoCriteria registerCriteriaChainElement(MongoCriteria criteria) {

		if (lastOperatorWasNot()) {
			throw new IllegalArgumentException(
					"operator $not is not allowed around criteria chain element: " + criteria.getCriteriaObject());
		} else {
			criteriaChain.add(criteria);
		}
		return this;
	}

	@Override
	public String getKey() {
		return this.key;
	}

	public Document getCriteriaObject() {

		if (this.criteriaChain.size() == 1) {
			return criteriaChain.get(0).getSingleCriteriaObject();
		} else if (CollectionUtils.isEmpty(this.criteriaChain) && !CollectionUtils.isEmpty(this.criteria)) {
			return getSingleCriteriaObject();
		} else {
			Document criteriaObject = new Document();
			for (MongoCriteria c : this.criteriaChain) {
				Document document = c.getSingleCriteriaObject();
				for (String k : document.keySet()) {
					setValue(criteriaObject, k, document.get(k));
				}
			}
			return criteriaObject;
		}
	}

	protected Document getSingleCriteriaObject() {

		Document document = new Document();
		boolean not = false;

		for (Entry<String, Object> entry : criteria.entrySet()) {

			String key = entry.getKey();
			Object value = entry.getValue();

			if (not) {
				Document notDocument = new Document();
				notDocument.put(key, value);
				document.put("$not", notDocument);
				not = false;
			} else {
				if ("$not".equals(key) && value == null) {
					not = true;
				} else {
					document.put(key, value);
				}
			}
		}

		if (!StringUtils.hasText(this.key)) {
			if (not) {
				return new Document("$not", document);
			}
			return document;
		}

		Document queryCriteria = new Document();

		if (!NOT_SET.equals(isValue)) {
			queryCriteria.put(this.key, this.isValue);
			queryCriteria.putAll(document);
		} else {
			queryCriteria.put(this.key, document);
		}

		return queryCriteria;
	}

	private BasicDBList createCriteriaList(NoSQLCriteria<?>[] criteria) {
		BasicDBList bsonList = new BasicDBList();
		for (NoSQLCriteria<?> c : criteria) {
			bsonList.add(c.getCriteriaObject());
		}
		return bsonList;
	}

	private void setValue(Document document, String key, Object value) {
		Object existing = document.get(key);
		if (existing == null) {
			document.put(key, value);
		} else {
			throw new InvalidMongoDbApiUsageException("Due to limitations of the com.mongodb.BasicDocument, "
					+ "you can't add a second '" + key + "' expression specified as '" + key + " : " + value + "'. "
					+ "Criteria already contains '" + key + " : " + existing + "'.");
		}
	}

	

	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}

		if (obj == null || !getClass().equals(obj.getClass())) {
			return false;
		}

		MongoCriteria that = (MongoCriteria) obj;

		if (this.criteriaChain.size() != that.criteriaChain.size()) {
			return false;
		}

		for (int i = 0; i < this.criteriaChain.size(); i++) {

			MongoCriteria left = this.criteriaChain.get(i);
			MongoCriteria right = that.criteriaChain.get(i);

			if (!simpleCriteriaEquals(left, right)) {
				return false;
			}
		}

		return true;
	}

	private boolean simpleCriteriaEquals(MongoCriteria left, MongoCriteria right) {

		boolean keyEqual = left.key == null ? right.key == null : left.key.equals(right.key);
		boolean criteriaEqual = left.criteria.equals(right.criteria);
		boolean valueEqual = isEqual(left.isValue, right.isValue);

		return keyEqual && criteriaEqual && valueEqual;
	}

	private boolean isEqual(Object left, Object right) {

		if (left == null) {
			return right == null;
		}

		if (Pattern.class.isInstance(left)) {

			if (!Pattern.class.isInstance(right)) {
				return false;
			}

			Pattern leftPattern = (Pattern) left;
			Pattern rightPattern = (Pattern) right;

			return leftPattern.pattern().equals(rightPattern.pattern()) //
					&& leftPattern.flags() == rightPattern.flags();
		}

		return ObjectUtils.nullSafeEquals(left, right);
	}

	@Override
	public int hashCode() {

		int result = 17;

		result += nullSafeHashCode(key);
		result += criteria.hashCode();
		result += nullSafeHashCode(isValue);

		return result;
	}


	public interface BitwiseCriteriaOperators {

		MongoCriteria allClear(int numericBitmask);

		MongoCriteria allClear(String bitmask);

		MongoCriteria allClear(List<Integer> positions);

		MongoCriteria allSet(int numericBitmask);

		MongoCriteria allSet(String bitmask);

		MongoCriteria allSet(List<Integer> positions);

		MongoCriteria anyClear(int numericBitmask);

		MongoCriteria anyClear(String bitmask);

		MongoCriteria anyClear(List<Integer> positions);

		MongoCriteria anySet(int numericBitmask);

		MongoCriteria anySet(String bitmask);

		MongoCriteria anySet(List<Integer> positions);

	}

	private static class BitwiseCriteriaOperatorsImpl implements BitwiseCriteriaOperators {

		private final MongoCriteria target;

		BitwiseCriteriaOperatorsImpl(MongoCriteria target) {
			this.target = target;
		}

		@Override
		public MongoCriteria allClear(int numericBitmask) {
			return numericBitmask("$bitsAllClear", numericBitmask);
		}

		@Override
		public MongoCriteria allClear(String bitmask) {
			return stringBitmask("$bitsAllClear", bitmask);
		}

		@Override
		public MongoCriteria allClear(List<Integer> positions) {
			return positions("$bitsAllClear", positions);
		}

		@Override
		public MongoCriteria allSet(int numericBitmask) {
			return numericBitmask("$bitsAllSet", numericBitmask);
		}

		@Override
		public MongoCriteria allSet(String bitmask) {
			return stringBitmask("$bitsAllSet", bitmask);
		}

		@Override
		public MongoCriteria allSet(List<Integer> positions) {
			return positions("$bitsAllSet", positions);
		}

		@Override
		public MongoCriteria anyClear(int numericBitmask) {
			return numericBitmask("$bitsAnyClear", numericBitmask);
		}

		@Override
		public MongoCriteria anyClear(String bitmask) {
			return stringBitmask("$bitsAnyClear", bitmask);
		}

		@Override
		public MongoCriteria anyClear(List<Integer> positions) {
			return positions("$bitsAnyClear", positions);
		}

		@Override
		public MongoCriteria anySet(int numericBitmask) {
			return numericBitmask("$bitsAnySet", numericBitmask);
		}

		@Override
		public MongoCriteria anySet(String bitmask) {
			return stringBitmask("$bitsAnySet", bitmask);
		}

		@Override
		public MongoCriteria anySet(List<Integer> positions) {
			return positions("$bitsAnySet", positions);
		}

		private MongoCriteria positions(String operator, List<Integer> positions) {

			Assert.notNull(positions, "Positions must not be null!");
			Assert.noNullElements(positions.toArray(), "Positions must not contain null values.");

			target.criteria.put(operator, positions);
			return target;
		}

		private MongoCriteria stringBitmask(String operator, String bitmask) {

			Assert.hasText(bitmask, "Bitmask must not be null!");

			try {
				target.criteria.put(operator, new Binary(Base64.decode(bitmask)));
			} catch (IOException e) {
				throw new CriteriaException(e);
			}
			return target;
		}

		private MongoCriteria numericBitmask(String operator, int bitmask) {

			target.criteria.put(operator, bitmask);
			return target;
		}
	}
}
