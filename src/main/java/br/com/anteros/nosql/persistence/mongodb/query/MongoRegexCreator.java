/*
 * Copyright 2015-2018 the original author or authors.
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

import java.util.regex.Pattern;

public enum MongoRegexCreator {

	INSTANCE;

	public enum MatchMode {

		DEFAULT,
		EXACT,
		STARTING_WITH,
		ENDING_WITH,
		CONTAINING,
		REGEX,
		LIKE;
	}

	private static final Pattern PUNCTATION_PATTERN = Pattern.compile("\\p{Punct}");

	public String toRegularExpression(String source, MatchMode matcherType) {

		if (matcherType == null || source == null) {
			return source;
		}

		String regex = prepareAndEscapeStringBeforeApplyingLikeRegex(source, matcherType);

		switch (matcherType) {
			case STARTING_WITH:
				return String.format("^%s", regex);
			case ENDING_WITH:
				return String.format("%s$", regex);
			case CONTAINING:
				return String.format(".*%s.*", regex);
			case EXACT:
				return String.format("^%s$", regex);
			default:
				return regex;
		}
	}

	private String prepareAndEscapeStringBeforeApplyingLikeRegex(String source, MatchMode matcherType) {

		if (MatchMode.REGEX == matcherType) {
			return source;
		}

		if (MatchMode.LIKE != matcherType) {
			return PUNCTATION_PATTERN.matcher(source).find() ? Pattern.quote(source) : source;
		}

		if (source.equals("*")) {
			return ".*";
		}

		StringBuilder sb = new StringBuilder();

		boolean leadingWildcard = source.startsWith("*");
		boolean trailingWildcard = source.endsWith("*");

		String valueToUse = source.substring(leadingWildcard ? 1 : 0,
				trailingWildcard ? source.length() - 1 : source.length());

		if (PUNCTATION_PATTERN.matcher(valueToUse).find()) {
			valueToUse = Pattern.quote(valueToUse);
		}

		if (leadingWildcard) {
			sb.append(".*");
		}

		sb.append(valueToUse);

		if (trailingWildcard) {
			sb.append(".*");
		}

		return sb.toString();
	}
}
