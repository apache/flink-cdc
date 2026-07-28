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

/** Rewrites Transform SQL syntax that is not recognized by the bundled Calcite parser. */
final class TransformSqlSyntaxRewriter {

    private static final String TRY_CAST = "TRY_CAST";

    private TransformSqlSyntaxRewriter() {}

    static String rewriteTryCast(String sql) {
        return rewriteRange(sql, 0, sql.length());
    }

    private static String rewriteRange(String sql, int start, int end) {
        StringBuilder rewritten = new StringBuilder(end - start);
        int index = start;
        while (index < end) {
            int protectedEnd = findProtectedRegionEnd(sql, index, end);
            if (protectedEnd > index) {
                rewritten.append(sql, index, protectedEnd);
                index = protectedEnd;
                continue;
            }

            if (!matchesTryCast(sql, index, end)) {
                rewritten.append(sql.charAt(index));
                index++;
                continue;
            }

            int openingParenthesis = findOpeningParenthesis(sql, index + TRY_CAST.length(), end);
            if (openingParenthesis < 0) {
                rewritten.append(sql.charAt(index));
                index++;
                continue;
            }

            int closingParenthesis = findMatchingParenthesis(sql, openingParenthesis, end);
            if (closingParenthesis < 0) {
                rewritten.append(sql, index, end);
                break;
            }

            String operand = rewriteRange(sql, openingParenthesis + 1, closingParenthesis);
            rewritten.append(sql, index, openingParenthesis + 1);
            if (containsTopLevelAs(operand)) {
                rewritten.append("CAST(").append(operand).append(')');
            } else {
                rewritten.append(operand);
            }
            rewritten.append(')');
            index = closingParenthesis + 1;
        }
        return rewritten.toString();
    }

    private static boolean matchesTryCast(String sql, int index, int end) {
        int keywordEnd = index + TRY_CAST.length();
        return keywordEnd <= end
                && sql.regionMatches(true, index, TRY_CAST, 0, TRY_CAST.length())
                && (index == 0 || !isIdentifierPart(sql.charAt(index - 1)))
                && (keywordEnd == end || !isIdentifierPart(sql.charAt(keywordEnd)));
    }

    private static boolean isIdentifierPart(char character) {
        return Character.isLetterOrDigit(character) || character == '_' || character == '$';
    }

    private static int findOpeningParenthesis(String sql, int start, int end) {
        int index = start;
        while (index < end) {
            char character = sql.charAt(index);
            if (Character.isWhitespace(character)) {
                index++;
            } else if (isLineCommentStart(sql, index, end)) {
                index = findLineCommentEnd(sql, index, end);
            } else if (isBlockCommentStart(sql, index, end)) {
                index = findBlockCommentEnd(sql, index, end);
            } else {
                return character == '(' ? index : -1;
            }
        }
        return -1;
    }

    private static int findMatchingParenthesis(String sql, int openingParenthesis, int end) {
        int depth = 1;
        int index = openingParenthesis + 1;
        while (index < end) {
            int protectedEnd = findProtectedRegionEnd(sql, index, end);
            if (protectedEnd > index) {
                index = protectedEnd;
                continue;
            }

            char character = sql.charAt(index);
            if (character == '(') {
                depth++;
            } else if (character == ')' && --depth == 0) {
                return index;
            }
            index++;
        }
        return -1;
    }

    private static boolean containsTopLevelAs(String sql) {
        int depth = 0;
        int index = 0;
        while (index < sql.length()) {
            int protectedEnd = findProtectedRegionEnd(sql, index, sql.length());
            if (protectedEnd > index) {
                index = protectedEnd;
                continue;
            }

            char character = sql.charAt(index);
            if (character == '(') {
                depth++;
            } else if (character == ')') {
                depth--;
            } else if (depth == 0
                    && index + 2 <= sql.length()
                    && sql.regionMatches(true, index, "AS", 0, 2)
                    && (index == 0 || !isIdentifierPart(sql.charAt(index - 1)))
                    && (index + 2 == sql.length() || !isIdentifierPart(sql.charAt(index + 2)))) {
                return true;
            }
            index++;
        }
        return false;
    }

    private static int findProtectedRegionEnd(String sql, int index, int end) {
        char character = sql.charAt(index);
        if (character == '\'' || character == '"' || character == '`') {
            return findQuotedRegionEnd(sql, index, end, character);
        }
        if (isLineCommentStart(sql, index, end)) {
            return findLineCommentEnd(sql, index, end);
        }
        if (isBlockCommentStart(sql, index, end)) {
            return findBlockCommentEnd(sql, index, end);
        }
        return index;
    }

    private static int findQuotedRegionEnd(String sql, int start, int end, char quote) {
        int index = start + 1;
        while (index < end) {
            char character = sql.charAt(index);
            if (character == '\\' && index + 1 < end) {
                index += 2;
            } else if (character == quote) {
                if (index + 1 < end && sql.charAt(index + 1) == quote) {
                    index += 2;
                } else {
                    return index + 1;
                }
            } else {
                index++;
            }
        }
        return end;
    }

    private static boolean isLineCommentStart(String sql, int index, int end) {
        return index + 1 < end && sql.charAt(index) == '-' && sql.charAt(index + 1) == '-';
    }

    private static int findLineCommentEnd(String sql, int start, int end) {
        int index = start + 2;
        while (index < end && sql.charAt(index) != '\n' && sql.charAt(index) != '\r') {
            index++;
        }
        return index;
    }

    private static boolean isBlockCommentStart(String sql, int index, int end) {
        return index + 1 < end && sql.charAt(index) == '/' && sql.charAt(index + 1) == '*';
    }

    private static int findBlockCommentEnd(String sql, int start, int end) {
        int index = start + 2;
        while (index + 1 < end) {
            if (sql.charAt(index) == '*' && sql.charAt(index + 1) == '/') {
                return index + 2;
            }
            index++;
        }
        return end;
    }
}
