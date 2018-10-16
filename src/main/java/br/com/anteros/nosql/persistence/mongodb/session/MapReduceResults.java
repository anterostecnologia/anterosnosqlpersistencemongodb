/*
 * Copyright 2011-2018 the original author or authors.
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

import java.util.Iterator;
import java.util.List;

import org.bson.Document;

import com.mongodb.MapReduceOutput;

import br.com.anteros.core.utils.Assert;
import br.com.anteros.nosql.persistence.session.MapReduceTiming;


public class MapReduceResults<T> implements Iterable<T> {

	private final List<T> mappedResults;
	private final Document rawResults;
	private final String outputCollection;
	private final MapReduceTiming mapReduceTiming;
	private final MapReduceCounts mapReduceCounts;


	
	public MapReduceResults(List<T> mappedResults, Document rawResults) {

		Assert.notNull(mappedResults, "List of mapped results must not be null!");
		Assert.notNull(rawResults, "Raw results must not be null!");

		this.mappedResults = mappedResults;
		this.rawResults = rawResults;
		this.mapReduceTiming = parseTiming(rawResults);
		this.mapReduceCounts = parseCounts(rawResults);
		this.outputCollection = parseOutputCollection(rawResults);
	}


	public MapReduceResults(List<T> mappedResults, MapReduceOutput mapReduceOutput) {

		Assert.notNull(mappedResults, "MappedResults must not be null!");
		Assert.notNull(mapReduceOutput, "MapReduceOutput must not be null!");

		this.mappedResults = mappedResults;
		this.rawResults = null;
		this.mapReduceTiming = parseTiming(mapReduceOutput);
		this.mapReduceCounts = parseCounts(mapReduceOutput);
		this.outputCollection = parseOutputCollection(mapReduceOutput);
	}


	public Iterator<T> iterator() {
		return mappedResults.iterator();
	}

	public MapReduceTiming getTiming() {
		return mapReduceTiming;
	}

	public MapReduceCounts getCounts() {
		return mapReduceCounts;
	}

	public String getOutputCollection() {
		return outputCollection;
	}

	public Document getRawResults() {
		return rawResults;
	}

	private static MapReduceTiming parseTiming(Document rawResults) {

		Document timing = (Document) rawResults.get("timing");

		if (timing == null) {
			return new MapReduceTiming(-1, -1, -1);
		}

		if (timing.get("mapTime") != null && timing.get("emitLoop") != null && timing.get("total") != null) {
			return new MapReduceTiming(getAsLong(timing, "mapTime"), getAsLong(timing, "emitLoop"),
					getAsLong(timing, "total"));
		}

		return new MapReduceTiming(-1, -1, -1);
	}


	private static Long getAsLong(Document source, String key) {

		Object raw = source.get(key);

		return raw instanceof Long ? (Long) raw : (Integer) raw;
	}


	private static MapReduceCounts parseCounts(Document rawResults) {

		Document counts = (Document) rawResults.get("counts");

		if (counts == null) {
			return MapReduceCounts.NONE;
		}

		if (counts.get("input") != null && counts.get("emit") != null && counts.get("output") != null) {
			return new MapReduceCounts(getAsLong(counts, "input"), getAsLong(counts, "emit"), getAsLong(counts, "output"));
		}

		return MapReduceCounts.NONE;
	}


	private static String parseOutputCollection(Document rawResults) {

		Object resultField = rawResults.get("result");

		if (resultField == null) {
			return null;
		}

		return resultField instanceof Document ? ((Document) resultField).get("collection").toString()
				: resultField.toString();
	}

	private static MapReduceCounts parseCounts(final MapReduceOutput mapReduceOutput) {
		return new MapReduceCounts(mapReduceOutput.getInputCount(), mapReduceOutput.getEmitCount(),
				mapReduceOutput.getOutputCount());
	}

	private static String parseOutputCollection(final MapReduceOutput mapReduceOutput) {
		return mapReduceOutput.getCollectionName();
	}

	private static MapReduceTiming parseTiming(MapReduceOutput mapReduceOutput) {
		return new MapReduceTiming(-1, -1, mapReduceOutput.getDuration());
	}
}
