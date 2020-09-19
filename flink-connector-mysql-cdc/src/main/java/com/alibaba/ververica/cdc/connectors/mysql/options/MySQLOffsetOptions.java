/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cdc.connectors.mysql.options;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Offset option for MySql.
 */
public class MySQLOffsetOptions {

	@Nullable
	private final String sourceOffsetFile;
	@Nullable
	private final Integer sourceOffsetPosition;

	private MySQLOffsetOptions(@Nullable String sourceOffsetFile, @Nullable Integer sourceOffsetPosition) {
		this.sourceOffsetFile = sourceOffsetFile;
		this.sourceOffsetPosition = sourceOffsetPosition;
	}

	@Nullable
	public String getSourceOffsetFile() {
		return sourceOffsetFile;
	}

	@Nullable
	public Integer getSourceOffsetPosition() {
		return sourceOffsetPosition;
	}

	@Override
	public String toString() {
		return "MySQLOffsetOptions{" +
				"sourceOffsetFile='" + sourceOffsetFile + '\'' +
				", sourceOffsetPosition='" + sourceOffsetPosition + '\'' +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		MySQLOffsetOptions that = (MySQLOffsetOptions) o;
		return Objects.equals(sourceOffsetFile, that.sourceOffsetFile) &&
				Objects.equals(sourceOffsetPosition, that.sourceOffsetPosition);
	}

	@Override
	public int hashCode() {
		return Objects.hash(sourceOffsetFile, sourceOffsetPosition);
	}

	/**
	 * Creates a builder of {@link MySQLOffsetOptions}.
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for {@link MySQLOffsetOptions}.
	 */
	public static class Builder {

		private String sourceOffsetFile;
		private Integer sourceOffsetPosition;

		/**
		 * Sets the MySql source offset file name.
		 */
		public Builder sourceOffsetFile(String sourceOffsetFile) {
			this.sourceOffsetFile = sourceOffsetFile;
			return this;
		}

		/**
		 * Sets the MySql source offset position.
		 */
		public Builder sourceOffsetPosition(Integer sourceOffsetPosition) {
			this.sourceOffsetPosition = sourceOffsetPosition;
			return this;
		}

		/**
		 * Creates an instance of {@link MySQLOffsetOptions}.
		 */
		public MySQLOffsetOptions build() {
			return new MySQLOffsetOptions(sourceOffsetFile, sourceOffsetPosition);
		}
	}
}
