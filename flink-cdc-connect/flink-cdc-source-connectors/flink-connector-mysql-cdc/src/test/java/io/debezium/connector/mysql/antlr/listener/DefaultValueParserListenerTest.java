/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import io.debezium.relational.Column;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link DefaultValueParserListener}. */
public class DefaultValueParserListenerTest {

    @Test
    public void testUnquoteWithSingleQuotes() throws Exception {
        DefaultValueParserListener listener =
                new DefaultValueParserListener(
                        Column.editor().name("test_col"), new AtomicReference<>(Boolean.FALSE));

        // Use reflection to access private unquote method
        Method unquoteMethod =
                DefaultValueParserListener.class.getDeclaredMethod("unquote", String.class);
        unquoteMethod.setAccessible(true);

        assertThat(unquoteMethod.invoke(listener, "'test value'")).isEqualTo("test value");
        assertThat(unquoteMethod.invoke(listener, "'0'")).isEqualTo("0");
        assertThat(unquoteMethod.invoke(listener, "''")).isEqualTo("");
    }

    @Test
    public void testUnquoteWithDoubleQuotes() throws Exception {
        DefaultValueParserListener listener =
                new DefaultValueParserListener(
                        Column.editor().name("test_col"), new AtomicReference<>(Boolean.FALSE));

        Method unquoteMethod =
                DefaultValueParserListener.class.getDeclaredMethod("unquote", String.class);
        unquoteMethod.setAccessible(true);

        assertThat(unquoteMethod.invoke(listener, "\"test value\"")).isEqualTo("test value");
        assertThat(unquoteMethod.invoke(listener, "\"0\"")).isEqualTo("0");
        assertThat(unquoteMethod.invoke(listener, "\"\"")).isEqualTo("");
    }

    @Test
    public void testUnquoteWithoutQuotes() throws Exception {
        DefaultValueParserListener listener =
                new DefaultValueParserListener(
                        Column.editor().name("test_col"), new AtomicReference<>(Boolean.FALSE));

        Method unquoteMethod =
                DefaultValueParserListener.class.getDeclaredMethod("unquote", String.class);
        unquoteMethod.setAccessible(true);

        assertThat(unquoteMethod.invoke(listener, "test value")).isEqualTo("test value");
        assertThat(unquoteMethod.invoke(listener, "0")).isEqualTo("0");
    }

    @Test
    public void testUnquoteWithMismatchedQuotes() throws Exception {
        DefaultValueParserListener listener =
                new DefaultValueParserListener(
                        Column.editor().name("test_col"), new AtomicReference<>(Boolean.FALSE));

        Method unquoteMethod =
                DefaultValueParserListener.class.getDeclaredMethod("unquote", String.class);
        unquoteMethod.setAccessible(true);

        // Mismatched quotes should return original string
        assertThat(unquoteMethod.invoke(listener, "'test value\"")).isEqualTo("'test value\"");
        assertThat(unquoteMethod.invoke(listener, "\"test value'")).isEqualTo("\"test value'");
    }

    @Test
    public void testUnquoteWithNullOrEmpty() throws Exception {
        DefaultValueParserListener listener =
                new DefaultValueParserListener(
                        Column.editor().name("test_col"), new AtomicReference<>(Boolean.FALSE));

        Method unquoteMethod =
                DefaultValueParserListener.class.getDeclaredMethod("unquote", String.class);
        unquoteMethod.setAccessible(true);

        assertThat(unquoteMethod.invoke(listener, (String) null)).isNull();
        assertThat(unquoteMethod.invoke(listener, "")).isEqualTo("");
        assertThat(unquoteMethod.invoke(listener, "'")).isEqualTo("'");
        assertThat(unquoteMethod.invoke(listener, "\"")).isEqualTo("\"");
    }
}
