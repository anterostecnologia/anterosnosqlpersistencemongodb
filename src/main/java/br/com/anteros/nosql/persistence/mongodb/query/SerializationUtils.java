/*
 * Copyright 2012-2018 the original author or authors.
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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.bson.Document;

import com.mongodb.util.JSON;


public abstract class SerializationUtils {

	private SerializationUtils() {

	}

	public static Map<String, Object> flattenMap(Document source) {

		if (source == null) {
			return Collections.emptyMap();
		}

		Map<String, Object> result = new LinkedHashMap<>();
		toFlatMap("", source, result);
		return result;
	}

	private static void toFlatMap(String currentPath, Object source, Map<String, Object> map) {

		if (source instanceof Document) {

			Document document = (Document) source;
			Iterator<Map.Entry<String, Object>> it = document.entrySet().iterator();
			String pathPrefix = currentPath.isEmpty() ? "" : currentPath + '.';

			while (it.hasNext()) {

				Map.Entry<String, Object> entry = it.next();

				if (entry.getKey().startsWith("$")) {
					if (map.containsKey(currentPath)) {
						((Document) map.get(currentPath)).put(entry.getKey(), entry.getValue());
					} else {
						map.put(currentPath, new Document(entry.getKey(), entry.getValue()));
					}
				} else {

					toFlatMap(pathPrefix + entry.getKey(), entry.getValue(), map);
				}
			}
		} else {
			map.put(currentPath, source);
		}
	}

	public static String serializeToJsonSafely(Object value) {

		if (value == null) {
			return null;
		}

		try {
			return value instanceof Document ? ((Document) value).toJson() : JSON.serialize(value);
		} catch (Exception e) {

			if (value instanceof Collection) {
				return toString((Collection<?>) value);
			} else if (value instanceof Map) {
				return toString((Map<?, ?>) value);
			} else {
				return String.format("{ \"$java\" : %s }", value.toString());
			}
		}
	}

	private static String toString(Map<?, ?> source) {		
		StringBuilder builder = new StringBuilder("");
		boolean appendDelimiter = false;
		for (Object key : source.keySet()) {
			if (appendDelimiter) {
				builder.append(", ");
			}
			builder.append("{ ");
			builder.append(serializeToJsonSafely(key));
			builder.append(serializeToJsonSafely(source.get(key)));
			builder.append("} ");
			appendDelimiter = true;
		}
		return builder.toString();
	}

	private static String toString(Collection<?> source) {
		StringBuilder builder = new StringBuilder("");
		boolean appendDelimiter = false;
		for (Object value : source) {
			if (appendDelimiter) {
				builder.append(", ");
			}
			builder.append("[ ");
			builder.append(serializeToJsonSafely(value));
			builder.append("] ");
			appendDelimiter = true;
		}
		return builder.toString();
	}

}
