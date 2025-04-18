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

package org.apache.flink.cdc.connectors.oracle.source.converters;

/**
 * OracleGeometry represents a geometry object in Oracle Spatial. It contains the following fields:
 */
public class OracleGeometry {
    private static final byte[] WKB_EMPTY_GEOMETRYCOLLECTION = {
        0x01, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    };
    private int gtype;
    private int srid;
    private String geometryType;
    private double[] coordinates;
    private int[] elemInfo;
    private double[] ordinates;
    private byte[] wkb;

    public OracleGeometry() {}

    public OracleGeometry(int gtype, int srid, byte[] wkb, String geometryType) {
        this.gtype = gtype;
        this.srid = srid;
        this.geometryType = geometryType;
        this.wkb = wkb;
    }

    // Getter and Setter methods
    public int getGtype() {
        return gtype;
    }

    public void setGtype(int gtype) {
        this.gtype = gtype;
    }

    public int getSrid() {
        return srid;
    }

    public void setSrid(int srid) {
        this.srid = srid;
    }

    public String getGeometryType() {
        return geometryType;
    }

    public void setGeometryType(String geometryType) {
        this.geometryType = geometryType;
    }

    public double[] getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(double[] coordinates) {
        this.coordinates = coordinates;
    }

    public int[] getElemInfo() {
        return elemInfo;
    }

    public void setElemInfo(int[] elemInfo) {
        this.elemInfo = elemInfo;
    }

    public double[] getOrdinates() {
        return ordinates;
    }

    public void setOrdinates(double[] ordinates) {
        this.ordinates = ordinates;
    }

    public byte[] getWkb() {
        return wkb;
    }

    public void setWkb(byte[] wkb) {
        this.wkb = wkb;
    }

    public static OracleGeometry createEmpty() {
        return new OracleGeometry(0, 0, WKB_EMPTY_GEOMETRYCOLLECTION, null);
    }
}
