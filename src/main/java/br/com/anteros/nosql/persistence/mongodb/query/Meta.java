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

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import br.com.anteros.core.utils.Assert;
import br.com.anteros.core.utils.ObjectUtils;
import br.com.anteros.core.utils.StringUtils;

public class Meta {

	private enum MetaKey {
		MAX_TIME_MS("$maxTimeMS"), MAX_SCAN("$maxScan"), COMMENT("$comment"), SNAPSHOT("$snapshot");

		private String key;

		MetaKey(String key) {
			this.key = key;
		}
	}

	private final Map<String, Object> values = new LinkedHashMap<String, Object>(2);
	private final Set<CursorOption> flags = new LinkedHashSet<CursorOption>();
	private Integer cursorBatchSize;

	public Long getMaxTimeMsec() {
		return getValue(MetaKey.MAX_TIME_MS.key);
	}

	public void setMaxTimeMsec(long maxTimeMsec) {
		setMaxTime(Duration.ofMillis(maxTimeMsec));
	}

	public void setMaxTime(Duration timeout) {
		Assert.notNull(timeout, "Timeout must not be null!");
		setValue(MetaKey.MAX_TIME_MS.key, timeout.toMillis());
	}

	public Long getMaxScan() {
		return getValue(MetaKey.MAX_SCAN.key);
	}

	public void setComment(String comment) {
		setValue(MetaKey.COMMENT.key, comment);
	}

	public String getComment() {
		return getValue(MetaKey.COMMENT.key);
	}

	public boolean getSnapshot() {
		return getValue(MetaKey.SNAPSHOT.key, false);
	}

	public Integer getCursorBatchSize() {
		return cursorBatchSize;
	}

	public void setCursorBatchSize(int cursorBatchSize) {
		this.cursorBatchSize = cursorBatchSize;
	}

	public boolean addFlag(CursorOption option) {
		Assert.notNull(option, "CursorOption must not be null!");
		return this.flags.add(option);
	}

	public Set<CursorOption> getFlags() {
		return flags;
	}

	public boolean hasValues() {
		return !this.values.isEmpty() || !this.flags.isEmpty() || this.cursorBatchSize != null;
	}

	public Iterable<Entry<String, Object>> values() {
		return Collections.unmodifiableSet(this.values.entrySet());
	}

	private void setValue(String key, Object value) {

		Assert.hasText(key, "Meta key must not be 'null' or blank.");

		if (value == null || (value instanceof String && !StringUtils.hasText((String) value))) {
			this.values.remove(key);
		}
		this.values.put(key, value);
	}

	
	@SuppressWarnings("unchecked")
	private <T> T getValue(String key) {
		return (T) this.values.get(key);
	}

	private <T> T getValue(String key, T defaultValue) {

		T value = getValue(key);
		return value != null ? value : defaultValue;
	}

	@Override
	public int hashCode() {
		int hash = ObjectUtils.nullSafeHashCode(this.values);
		hash += ObjectUtils.nullSafeHashCode(this.flags);
		return hash;
	}

	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}

		if (!(obj instanceof Meta)) {
			return false;
		}

		Meta other = (Meta) obj;
		if (!ObjectUtils.nullSafeEquals(this.values, other.values)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(this.flags, other.flags);
	}

	public enum CursorOption {

		NO_TIMEOUT,
		EXHAUST,
		SLAVE_OK,
		PARTIAL
	}
}
