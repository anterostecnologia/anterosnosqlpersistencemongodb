/*
 * Copyright 2014-2018 the original author or authors.
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



public class Term {

	public enum Type {
		WORD, PHRASE;
	}

	private final Type type;
	private final String raw;
	private boolean negated;

	public Term(String raw) {
		this(raw, Type.WORD);
	}

	public Term(String raw, Type type) {
		this.raw = raw;
		this.type = type == null ? Type.WORD : type;
	}

	public Term negate() {
		this.negated = true;
		return this;
	}

	public boolean isNegated() {
		return negated;
	}

	public Type getType() {
		return type;
	}

	public String getFormatted() {

		String formatted = Type.PHRASE.equals(type) ? quotePhrase(raw) : raw;
		return negated ? negateRaw(formatted) : formatted;
	}

	@Override
	public String toString() {
		return getFormatted();
	}

	protected String quotePhrase(String raw) {
		return "\"" + raw + "\"";
	}

	protected String negateRaw(String raw) {
		return "-" + raw;
	}
}
