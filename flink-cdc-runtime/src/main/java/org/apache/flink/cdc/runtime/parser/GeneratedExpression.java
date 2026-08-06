/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.runtime.parser;

import java.io.Serializable;
import java.util.Objects;

/** Statement-level Java code generated from a transform expression. */
public class GeneratedExpression implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String code;
    private final String resultTerm;
    private final Class<?> resultClass;

    private GeneratedExpression(String code, String resultTerm, Class<?> resultClass) {
        this.code = Objects.requireNonNull(code);
        this.resultTerm = Objects.requireNonNull(resultTerm);
        this.resultClass = Objects.requireNonNull(resultClass);
    }

    public String getCode() {
        return code;
    }

    public String getResultTerm() {
        return resultTerm;
    }

    public Class<?> getResultClass() {
        return resultClass;
    }

    public String asScript() {
        StringBuilder script = new StringBuilder();
        appendCode(script, code);
        script.append("return ").append(resultTerm).append(";");
        return script.toString();
    }

    public static GeneratedExpression of(String code, String resultTerm, Class<?> resultClass) {
        return new GeneratedExpression(code, resultTerm, resultClass);
    }

    public static GeneratedExpression fromExpression(String resultTerm, Class<?> resultClass) {
        return of("", resultTerm, resultClass);
    }

    private static void appendCode(StringBuilder builder, String code) {
        if (code.isEmpty()) {
            return;
        }
        builder.append(code);
        if (code.charAt(code.length() - 1) != '\n') {
            builder.append('\n');
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GeneratedExpression that = (GeneratedExpression) o;
        return code.equals(that.code)
                && resultTerm.equals(that.resultTerm)
                && resultClass.equals(that.resultClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code, resultTerm, resultClass);
    }

    @Override
    public String toString() {
        return "GeneratedExpression{"
                + "code='"
                + code
                + '\''
                + ", resultTerm='"
                + resultTerm
                + '\''
                + ", resultClass="
                + resultClass
                + '}';
    }
}
