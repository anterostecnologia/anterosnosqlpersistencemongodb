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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.bson.Document;

import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceCommand.OutputType;
import com.mongodb.client.model.MapReduceAction;


public class MapReduceOptions {

	private String outputCollection;

	private Optional<String> outputDatabase = Optional.empty();
	private MapReduceCommand.OutputType outputType = MapReduceCommand.OutputType.REPLACE;
	private Map<String, Object> scopeVariables = new HashMap<>();
	private Map<String, Object> extraOptions = new HashMap<>();
	private Boolean jsMode;
	private Boolean verbose = Boolean.TRUE;
	private Integer limit;

	private Optional<Boolean> outputSharded = Optional.empty();
	private Optional<String> finalizeFunction = Optional.empty();


	public static MapReduceOptions options() {
		return new MapReduceOptions();
	}

	public MapReduceOptions limit(int limit) {

		this.limit = limit;
		return this;
	}

	public MapReduceOptions outputCollection(String collectionName) {

		this.outputCollection = collectionName;
		return this;
	}

	public MapReduceOptions outputDatabase(String outputDatabase) {

		this.outputDatabase = Optional.ofNullable(outputDatabase);
		return this;
	}

	public MapReduceOptions outputTypeInline() {

		this.outputType = MapReduceCommand.OutputType.INLINE;
		return this;
	}

	public MapReduceOptions outputTypeMerge() {

		this.outputType = MapReduceCommand.OutputType.MERGE;
		return this;
	}

	public MapReduceOptions outputTypeReduce() {
		this.outputType = MapReduceCommand.OutputType.REDUCE;
		return this;
	}

	public MapReduceOptions outputTypeReplace() {

		this.outputType = MapReduceCommand.OutputType.REPLACE;
		return this;
	}

	public MapReduceOptions outputSharded(boolean outputShared) {

		this.outputSharded = Optional.of(outputShared);
		return this;
	}

	public MapReduceOptions finalizeFunction(String finalizeFunction) {

		this.finalizeFunction = Optional.ofNullable(finalizeFunction);
		return this;
	}

	public MapReduceOptions scopeVariables(Map<String, Object> scopeVariables) {

		this.scopeVariables = scopeVariables;
		return this;
	}


	public MapReduceOptions javaScriptMode(boolean javaScriptMode) {

		this.jsMode = javaScriptMode;
		return this;
	}

	public MapReduceOptions verbose(boolean verbose) {

		this.verbose = verbose;
		return this;
	}

	public MapReduceOptions extraOption(String key, Object value) {

		extraOptions.put(key, value);
		return this;
	}

	public Map<String, Object> getExtraOptions() {
		return extraOptions;
	}

	public Optional<String> getFinalizeFunction() {
		return this.finalizeFunction;
	}

	
	public Boolean getJavaScriptMode() {
		return this.jsMode;
	}

	
	public String getOutputCollection() {
		return this.outputCollection;
	}

	public Optional<String> getOutputDatabase() {
		return this.outputDatabase;
	}

	public Optional<Boolean> getOutputSharded() {
		return this.outputSharded;
	}

	public MapReduceCommand.OutputType getOutputType() {
		return this.outputType;
	}

	public Map<String, Object> getScopeVariables() {
		return this.scopeVariables;
	}

	public Integer getLimit() {
		return limit;
	}

	public MapReduceAction getMapReduceAction() {

		switch (outputType) {
			case MERGE:
				return MapReduceAction.MERGE;
			case REDUCE:
				return MapReduceAction.REDUCE;
			case REPLACE:
				return MapReduceAction.REPLACE;
			case INLINE:
				return null;
			default:
				throw new IllegalStateException(String.format("Unknown output type %s for map reduce command.", outputType));
		}
	}

	public boolean usesInlineOutput() {
		return OutputType.INLINE.equals(outputType);
	}

	public Document getOptionsObject() {

		Document cmd = new Document();

		if (verbose != null) {
			cmd.put("verbose", verbose);
		}

		cmd.put("out", createOutObject());

		finalizeFunction.ifPresent(val -> cmd.append("finalize", val));

		if (scopeVariables != null) {
			cmd.put("scope", scopeVariables);
		}

		if (limit != null) {
			cmd.put("limit", limit);
		}

		if (!extraOptions.keySet().isEmpty()) {
			cmd.putAll(extraOptions);
		}

		return cmd;
	}

	protected Document createOutObject() {

		Document out = new Document();

		switch (getOutputType()) {
			case INLINE:
				out.put("inline", 1);
				break;
			case REPLACE:
				out.put("replace", outputCollection);
				break;
			case MERGE:
				out.put("merge", outputCollection);
				break;
			case REDUCE:
				out.put("reduce", outputCollection);
				break;
		}

		outputDatabase.ifPresent(val -> out.append("db", val));
		outputSharded.ifPresent(val -> out.append("sharded", val));

		return out;
	}
}
