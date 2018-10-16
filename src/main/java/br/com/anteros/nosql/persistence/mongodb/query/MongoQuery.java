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

import static br.com.anteros.core.utils.ObjectUtils.nullSafeEquals;
import static br.com.anteros.core.utils.ObjectUtils.nullSafeHashCode;
import static br.com.anteros.nosql.persistence.mongodb.query.SerializationUtils.serializeToJsonSafely;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.Document;

import br.com.anteros.core.utils.Assert;
import br.com.anteros.nosql.persistence.session.query.NoSQLQuery;
import br.com.anteros.nosql.persistence.session.query.Pageable;
import br.com.anteros.nosql.persistence.session.query.Sort;
import br.com.anteros.nosql.persistence.session.query.Sort.Order;

public class MongoQuery implements NoSQLQuery<Document> {

	private static final String RESTRICTED_TYPES_KEY = "_$RESTRICTED_TYPES";

	private final Set<Class<?>> restrictedTypes = new HashSet<>();
	private final Map<String, CriteriaDefinition> criteria = new LinkedHashMap<>();
	private Field fieldSpec = null;
	private Sort sort = Sort.unsorted();
	private long offset;
	private int limit;
	private String hint;
	private Document userDocument;

	private Meta meta = new Meta();

	public static  MongoQuery of(CriteriaDefinition criteriaDefinition) {
		return new MongoQuery(criteriaDefinition);
	}
	
	public static  MongoQuery of(Document document) {
		return new MongoQuery(document);
	}
	
	public static MongoQuery of() {
		return new MongoQuery();
	}

	private MongoQuery() {
	}

	private MongoQuery(CriteriaDefinition criteriaDefinition) {
		addCriteria(criteriaDefinition);
	}
	
	private MongoQuery(Document document) {
		this.userDocument = document;
	}

	public MongoQuery addCriteria(CriteriaDefinition criteriaDefinition) {

		CriteriaDefinition existing = this.criteria.get(criteriaDefinition.getKey());
		String key = criteriaDefinition.getKey();

		if (existing == null) {
			this.criteria.put(key, criteriaDefinition);
		} else {
			throw new InvalidMongoDbApiUsageException(String.format(
					"Due to limitations of the com.mongodb.BasicDocument, you can't add a second '%s' criteria. "
							+ "Query already contains '%s'",
					key, serializeToJsonSafely(existing.getCriteriaObject())));
		}

		return this;
	}

	public Field fields() {

		if (this.fieldSpec == null) {
			this.fieldSpec = new Field();
		}

		return this.fieldSpec;
	}

	public MongoQuery offSet(long offSet) {
		this.offset = offSet;
		return this;
	}

	public MongoQuery limit(int limit) {
		this.limit = limit;
		return this;
	}

	public MongoQuery withHint(String name) {
		Assert.hasText(name, "Hint must not be empty or null!");
		this.hint = name;
		return this;
	}

	public MongoQuery with(Pageable pageable) {

		if (pageable.isUnpaged()) {
			return this;
		}

		this.limit = pageable.getPageSize();
		this.offset = pageable.getOffset();

		return with(pageable.getSort());
	}

	public MongoQuery with(Sort sort) {

		Assert.notNull(sort, "Sort must not be null!");

		if (sort.isUnsorted()) {
			return this;
		}
		
		Iterator<Order> it = sort.iterator();
		
		while (it.hasNext()) {
			Order order = it.next();
			if (order.isIgnoreCase()) {
				throw new IllegalArgumentException(String.format("Given sort contained an Order for %s with ignore case! "
						+ "MongoDB does not support sorting ignoring case currently!", order.getProperty()));
			}
		}

		this.sort = this.sort.and(sort);

		return this;
	}

	public Set<Class<?>> getRestrictedTypes() {
		return restrictedTypes;
	}

	public MongoQuery restrict(Class<?> type, Class<?>... additionalTypes) {

		Assert.notNull(type, "Type must not be null!");
		Assert.notNull(additionalTypes, "AdditionalTypes must not be null");

		restrictedTypes.add(type);
		restrictedTypes.addAll(Arrays.asList(additionalTypes));

		return this;
	}

	public Document getQueryObject() {
		if (userDocument!=null)
			return userDocument;

		Document document = new Document();

		for (CriteriaDefinition definition : criteria.values()) {
			document.putAll(definition.getCriteriaObject());
		}

		if (!restrictedTypes.isEmpty()) {
			document.put(RESTRICTED_TYPES_KEY, getRestrictedTypes());
		}

		return document;
	}

	public Document getFieldsObject() {
		Document result = this.fieldSpec == null ? new Document() : fieldSpec.getFieldsObject();
		return result;
	}

	public Document getSortObject() {

		if (this.sort.isUnsorted()) {
			return new Document();
		}

		Document document = new Document();
		
		Iterator<Order> it = this.sort.iterator();

		while (it.hasNext()) {
			Order order = it.next();
			document.put(order.getProperty(), order.isAscending() ? 1 : -1);
		}

		return document;
	}

	public long getOffset() {
		return this.offset;
	}

	public int getLimit() {
		return this.limit;
	}

	public String getHint() {
		return hint;
	}

	public MongoQuery maxTimeMsec(long maxTimeMsec) {
		meta.setMaxTimeMsec(maxTimeMsec);
		return this;
	}

	public MongoQuery maxTime(Duration timeout) {
		meta.setMaxTime(timeout);
		return this;
	}

	public MongoQuery comment(String comment) {
		meta.setComment(comment);
		return this;
	}

	public MongoQuery cursorBatchSize(int batchSize) {
		meta.setCursorBatchSize(batchSize);
		return this;
	}

	public MongoQuery noCursorTimeout() {
		meta.addFlag(Meta.CursorOption.NO_TIMEOUT);
		return this;
	}

	public MongoQuery exhaust() {
		meta.addFlag(Meta.CursorOption.EXHAUST);
		return this;
	}

	public MongoQuery slaveOk() {
		meta.addFlag(Meta.CursorOption.SLAVE_OK);
		return this;
	}

	public MongoQuery partialResults() {
		meta.addFlag(Meta.CursorOption.PARTIAL);
		return this;
	}

	public Meta getMeta() {
		return meta;
	}

	public void setMeta(Meta meta) {
		Assert.notNull(meta, "Query meta might be empty but must not be null.");
		this.meta = meta;
	}

	protected List<CriteriaDefinition> getCriteria() {
		return new ArrayList<>(this.criteria.values());
	}

	@Override
	public String toString() {
		return String.format("Query: %s, Fields: %s, Sort: %s", serializeToJsonSafely(getQueryObject()),
				serializeToJsonSafely(getFieldsObject()), serializeToJsonSafely(getSortObject()));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (obj == null || !getClass().equals(obj.getClass())) {
			return false;
		}

		return querySettingsEquals((MongoQuery) obj);
	}

	protected boolean querySettingsEquals(MongoQuery that) {

		boolean criteriaEqual = this.criteria.equals(that.criteria);
		boolean fieldsEqual = nullSafeEquals(this.fieldSpec, that.fieldSpec);
		boolean sortEqual = this.sort.equals(that.sort);
		boolean hintEqual = nullSafeEquals(this.hint, that.hint);
		boolean offSetEqual = this.offset == that.offset;
		boolean limitEqual = this.limit == that.limit;
		boolean metaEqual = nullSafeEquals(this.meta, that.meta);

		return criteriaEqual && fieldsEqual && sortEqual && hintEqual && offSetEqual && limitEqual && metaEqual;
	}

	@Override
	public int hashCode() {

		int result = 17;

		result += 31 * criteria.hashCode();
		result += 31 * nullSafeHashCode(fieldSpec);
		result += 31 * nullSafeHashCode(sort);
		result += 31 * nullSafeHashCode(hint);
		result += 31 * offset;
		result += 31 * limit;
		result += 31 * nullSafeHashCode(meta);

		return result;
	}


}
