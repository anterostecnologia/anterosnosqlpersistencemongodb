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
package br.com.anteros.nosql.persistence.mongodb.session;

import java.util.Optional;

import org.bson.Document;



public class GroupBy {

	private  Document initialDocument;
	private  String reduce;

	private Optional<Document> keys = Optional.empty();
	private Optional<String> keyFunction = Optional.empty();
	private Optional<String> initial = Optional.empty();
	private Optional<String> finalize = Optional.empty();


	public GroupBy(String... keys) {

		Document document = new Document();
		for (String key : keys) {
			document.put(key, 1);
		}

		this.keys = Optional.of(document);
	}



	public GroupBy( String key, boolean isKeyFunction) {

		Document document = new Document();
		if (isKeyFunction) {
			keyFunction = Optional.ofNullable(key);
		} else {
			document.put(key, 1);
			keys = Optional.of(document);
		}
	}


	public static GroupBy keyFunction(String key) {
		return new GroupBy(key, true);
	}


	public static GroupBy key(String... keys) {
		return new GroupBy(keys);
	}


	public GroupBy initialDocument( String initialDocument) {

		initial = Optional.ofNullable(initialDocument);
		return this;
	}


	public GroupBy initialDocument( Document initialDocument) {

		this.initialDocument = initialDocument;
		return this;
	}


	public GroupBy reduceFunction(String reduceFunction) {

		reduce = reduceFunction;
		return this;
	}


	public GroupBy finalizeFunction( String finalizeFunction) {

		finalize = Optional.ofNullable(finalizeFunction);
		return this;
	}

	
	public Document getGroupByObject() {

		Document document = new Document();

		keys.ifPresent(val -> document.append("key", val));
		keyFunction.ifPresent(val -> document.append("$keyf", val));

		document.put("$reduce", reduce);
		document.put("initial", initialDocument);

		initial.ifPresent(val -> document.append("initial", val));
		finalize.ifPresent(val -> document.append("finalize", val));

		return document;
	}

}
