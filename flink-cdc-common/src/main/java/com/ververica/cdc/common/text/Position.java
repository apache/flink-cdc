/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.common.text;

/**
 * A class that represents the position of a particular character in terms of the lines and columns
 * of a character sequence.
 */
public final class Position {

    /** The position is used when there is no content. */
    public static final Position EMPTY_CONTENT_POSITION = new Position(-1, 1, 0);

    private final int line;
    private final int column;
    private final int indexInContent;

    public Position(int indexInContent, int line, int column) {
        this.indexInContent = indexInContent < 0 ? -1 : indexInContent;
        this.line = line;
        this.column = column;

        assert this.line > 0;
        assert this.column >= 0;
        // make sure that negative index means an EMPTY_CONTENT_POSITION
        assert this.indexInContent >= 0 || this.line == 1 && this.column == 0;
    }

    /**
     * Get the 0-based index of this position in the content character array.
     *
     * @return the index; never negative except for the first position in an empty content.
     */
    public int index() {
        return indexInContent;
    }

    /**
     * Get the 1-based column number of the character.
     *
     * @return the column number; always positive
     */
    public int column() {
        return column;
    }

    /**
     * Get the 1-based line number of the character.
     *
     * @return the line number; always positive
     */
    public int line() {
        return line;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return indexInContent;
    }

    @Override
    public String toString() {
        return "" + indexInContent + ':' + line + ':' + column;
    }

    /**
     * Return a new position that is the addition of this position and that supplied.
     *
     * @param position the position to add to this object; may not be null
     * @return the combined position
     */
    public Position add(Position position) {
        if (this.index() < 0) {
            return position.index() < 0 ? EMPTY_CONTENT_POSITION : position;
        }

        if (position.index() < 0) {
            return this;
        }

        int index = this.index() + position.index();
        int line = position.line() + this.line() - 1;
        int column = this.line() == 1 ? this.column() + position.column() : this.column();

        return new Position(index, line, column);
    }
}
