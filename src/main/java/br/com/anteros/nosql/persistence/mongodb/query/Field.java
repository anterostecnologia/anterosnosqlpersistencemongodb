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



import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.bson.Document;

import br.com.anteros.core.utils.Assert;


public class Field {

	private final Map<String, Integer> criteria = new HashMap<String, Integer>();
	private final Map<String, Object> slices = new HashMap<String, Object>();
	private final Map<String, MongoCriteria> elemMatchs = new HashMap<String, MongoCriteria>();
	private String positionKey;
	private int positionValue;

	public Field include(String key) {
		criteria.put(key, Integer.valueOf(1));
		return this;
	}

	public Field exclude(String key) {
		criteria.put(key, Integer.valueOf(0));
		return this;
	}

	public Field slice(String key, int size) {
		slices.put(key, Integer.valueOf(size));
		return this;
	}

	public Field slice(String key, int offset, int size) {
		slices.put(key, new Integer[] { Integer.valueOf(offset), Integer.valueOf(size) });
		return this;
	}

	public Field elemMatch(String key, MongoCriteria elemMatchCriteria) {
		elemMatchs.put(key, elemMatchCriteria);
		return this;
	}

	public Field position(String field, int value) {

		Assert.hasText(field, "DocumentField must not be null or empty!");

		positionKey = field;
		positionValue = value;

		return this;
	}

	public Document getFieldsObject() {

		@SuppressWarnings({ "unchecked", "rawtypes" })
		Document document = new Document((Map) criteria);

		for (Entry<String, Object> entry : slices.entrySet()) {
			document.put(entry.getKey(), new Document("$slice", entry.getValue()));
		}

		for (Entry<String, MongoCriteria> entry : elemMatchs.entrySet()) {
			document.put(entry.getKey(), new Document("$elemMatch", entry.getValue().getCriteriaObject()));
		}

		if (positionKey != null) {
			document.put(positionKey + ".$", positionValue);
		}

		return document;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((criteria == null) ? 0 : criteria.hashCode());
		result = prime * result + ((elemMatchs == null) ? 0 : elemMatchs.hashCode());
		result = prime * result + ((positionKey == null) ? 0 : positionKey.hashCode());
		result = prime * result + positionValue;
		result = prime * result + ((slices == null) ? 0 : slices.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Field other = (Field) obj;
		if (criteria == null) {
			if (other.criteria != null)
				return false;
		} else if (!criteria.equals(other.criteria))
			return false;
		if (elemMatchs == null) {
			if (other.elemMatchs != null)
				return false;
		} else if (!elemMatchs.equals(other.elemMatchs))
			return false;
		if (positionKey == null) {
			if (other.positionKey != null)
				return false;
		} else if (!positionKey.equals(other.positionKey))
			return false;
		if (positionValue != other.positionValue)
			return false;
		if (slices == null) {
			if (other.slices != null)
				return false;
		} else if (!slices.equals(other.slices))
			return false;
		return true;
	}
}
