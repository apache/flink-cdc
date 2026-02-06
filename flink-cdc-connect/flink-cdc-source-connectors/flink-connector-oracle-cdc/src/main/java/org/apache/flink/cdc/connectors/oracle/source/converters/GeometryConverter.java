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

import oracle.sql.ARRAY;
import oracle.sql.STRUCT;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKBWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** This class converts Oracle STRUCT objects to JTS geometries. */
public class GeometryConverter {
    private static final Logger logger = LoggerFactory.getLogger(GeometryConverter.class);
    private final WKBWriter wkbWriter;
    private final GeometryFactory geometryFactory;

    public GeometryConverter() {
        this.wkbWriter = new WKBWriter();
        this.geometryFactory = new GeometryFactory();
    }

    /**
     * Convert oracle.sql.STRUCT to OracleGeometry.
     *
     * @param struct Oracle STRUCT object.
     * @return OracleGeometry.
     */
    public OracleGeometry convertSTRUCTToWKT(STRUCT struct) {
        if (struct == null) {
            logger.warn("The input STRUCT object is empty");
            return null;
        }
        try {
            Object[] attributes = struct.getAttributes();
            OracleGeometry oracleGeometry = parseSTRUCT(attributes);
            Geometry jtsGeometry = convertToJTSGeometry(oracleGeometry);
            byte[] wkt = wkbWriter.write(jtsGeometry);
            logger.info(
                    "Successfully converted Oracle STRUCT geometry data, type: {}, SRID: {}",
                    oracleGeometry.getGeometryType(),
                    oracleGeometry.getSrid());
            oracleGeometry.setWkb(wkt);
            return oracleGeometry;
        } catch (SQLException e) {
            logger.error("STRUCT attribute retrieval failed", e);
            throw new RuntimeException("Geometric data conversion failed", e);
        }
    }

    /** Parse Oracle STRUCT attributes into OracleGeometry. */
    private OracleGeometry parseSTRUCT(Object[] attributes) throws SQLException {
        OracleGeometry geometry = new OracleGeometry();
        if (attributes.length < 2) {
            return geometry;
        }
        if (attributes[0] instanceof Number) {
            geometry.setGtype(((Number) attributes[0]).intValue());
        }
        if (attributes[1] instanceof Number) {
            geometry.setSrid(((Number) attributes[1]).intValue());
        }
        Long sdoType = ((BigDecimal) attributes[0]).longValue();
        if (sdoType == 2001L) {
            parseSdoPoint(attributes[2], geometry);
        } else {
            // Processing SDO-ELEM.INFO and SDO-ORDINATES
            parseSdoElemInfo(attributes[3], geometry);
            parseSdoOrdinates(attributes[4], geometry);
        }
        return geometry;
    }

    /** Parse SDO_POINT attribute. */
    private void parseSdoPoint(Object sdoPoint, OracleGeometry geometry) {
        if (sdoPoint instanceof STRUCT) {
            try {
                STRUCT pointStruct = (STRUCT) sdoPoint;
                Object[] pointAttrs = pointStruct.getAttributes();
                if (pointAttrs != null && pointAttrs.length >= 2) {
                    double x = pointAttrs[0] != null ? ((Number) pointAttrs[0]).doubleValue() : 0.0;
                    double y = pointAttrs[1] != null ? ((Number) pointAttrs[1]).doubleValue() : 0.0;
                    geometry.setCoordinates(new double[] {x, y});
                    geometry.setGeometryType("POINT");
                }
            } catch (SQLException e) {
                logger.error("SDO-POINT parsing failed", e);
            }
        }
    }

    /** Parse SDO_ELEM_INFO attribute. */
    private void parseSdoElemInfo(Object sdoElemInfo, OracleGeometry geometry) {
        if (sdoElemInfo instanceof ARRAY) {
            try {
                ARRAY elemArray = (ARRAY) sdoElemInfo;
                Object[] elemValues = (Object[]) elemArray.getArray();
                if (elemValues != null) {
                    int[] elemInfo = new int[elemValues.length];
                    for (int i = 0; i < elemValues.length; i++) {
                        if (elemValues[i] instanceof Number) {
                            elemInfo[i] = ((Number) elemValues[i]).intValue();
                        }
                    }
                    geometry.setElemInfo(elemInfo);
                }
            } catch (SQLException e) {
                logger.error("SDO-ELEM.INFO parsing failed", e);
            }
        }
    }

    /** Parse SDO_ORDINATES attribute. */
    private void parseSdoOrdinates(Object sdoOrdinates, OracleGeometry geometry) {
        if (sdoOrdinates instanceof ARRAY) {
            try {
                ARRAY ordinatesArray = (ARRAY) sdoOrdinates;
                Object[] ordinateValues = (Object[]) ordinatesArray.getArray();
                if (ordinateValues != null) {
                    double[] ordinates = new double[ordinateValues.length];
                    for (int i = 0; i < ordinateValues.length; i++) {
                        if (ordinateValues[i] instanceof Number) {
                            ordinates[i] = ((Number) ordinateValues[i]).doubleValue();
                        }
                    }
                    geometry.setOrdinates(ordinates);
                    determineGeometryType(geometry);
                }
            } catch (SQLException e) {
                logger.error("SDO-ORDINATES parsing failed", e);
            }
        }
    }

    /** Determine the geometry type based on ELEM_INFO. */
    private void determineGeometryType(OracleGeometry geometry) {
        if (geometry.getElemInfo() == null) {
            return;
        }

        int interpretation = geometry.getGtype();
        switch (interpretation) {
            case 2001:
                geometry.setGeometryType("POINT");
                break;
            case 2002:
                geometry.setGeometryType("LINESTRING");
                break;
            case 2003:
                geometry.setGeometryType("POLYGON");
                break;
            case 2005:
                geometry.setGeometryType("MULTIPOINT");
                break;
            case 2006:
                geometry.setGeometryType("MULTILINESTRING");
                break;
            case 2007:
                geometry.setGeometryType("MULTIPOLYGON");
                break;
            case 2004:
                geometry.setGeometryType("GEOMETRYCOLLECTION");
                break;
        }
    }

    /** Convert OracleGeometry to JTS Geometry. */
    private Geometry convertToJTSGeometry(OracleGeometry oracleGeometry) {
        String geometryType = oracleGeometry.getGeometryType();

        switch (geometryType) {
            case "POINT":
                return createJTSPoint(oracleGeometry);
            case "MULTIPOINT":
                return createJTSMultiPoint(oracleGeometry);
            case "LINESTRING":
                return createJTSLineString(oracleGeometry);
            case "MULTILINESTRING":
                return createJTSMultiLineString(oracleGeometry);
            case "POLYGON":
                return createJTSPolygon(oracleGeometry);
            case "MULTIPOLYGON":
                return createJTSMultiPolygon(oracleGeometry);
            case "GEOMETRYCOLLECTION":
                return createJTSGeometryCollection(oracleGeometry);
            default:
                throw new IllegalArgumentException("Unsupported geometry types: " + geometryType);
        }
    }

    /** Create JTS point Geometry from OracleGeometry. */
    private Geometry createJTSPoint(OracleGeometry oracleGeometry) {

        double[] coords = oracleGeometry.getCoordinates();
        if (coords == null || coords.length < 2) {
            throw new IllegalArgumentException("Incomplete point coordinate data");
        }

        Coordinate coord = new Coordinate(coords[0], coords[1]);
        return geometryFactory.createPoint(coord);
    }

    /** Create JTS multiple point Geometry from OracleGeometry. */
    private Geometry createJTSMultiPoint(OracleGeometry oracleGeometry) {
        double[] ordinates = oracleGeometry.getOrdinates();
        if (ordinates == null || ordinates.length < 2) {
            throw new IllegalArgumentException("Incomplete multi-point set coordinate data");
        }

        List<Point> points = new ArrayList<>();
        for (int i = 0; i < ordinates.length; i += 2) {
            Coordinate coord = new Coordinate(ordinates[i], ordinates[i + 1]);
            points.add(geometryFactory.createPoint(coord));
        }
        return geometryFactory.createMultiPoint(points.toArray(new Point[0]));
    }

    /** Create JTS line string Geometry from OracleGeometry. */
    private Geometry createJTSLineString(OracleGeometry oracleGeometry) {
        double[] ordinates = oracleGeometry.getOrdinates();
        if (ordinates == null || ordinates.length < 4) {
            throw new IllegalArgumentException("Incomplete line coordinate data");
        }

        List<Coordinate> coords = new ArrayList<>();
        for (int i = 0; i < ordinates.length; i += 2) {
            coords.add(new Coordinate(ordinates[i], ordinates[i + 1]));
        }
        return geometryFactory.createLineString(coords.toArray(new Coordinate[0]));
    }

    /** Create JTS multiple line string Geometry from OracleGeometry. */
    private Geometry createJTSMultiLineString(OracleGeometry oracleGeometry) {
        int[] elemInfo = oracleGeometry.getElemInfo();
        double[] ordinates = oracleGeometry.getOrdinates();

        List<LineString> lineStrings = new ArrayList<>();

        for (int i = 0; i < elemInfo.length; i += 3) {
            int startIndex = elemInfo[i] - 1;
            int etype = elemInfo[i + 1];
            int interpretation = elemInfo[i + 2];
            int endIndex;

            // Calculate the end position of the current element
            if (i + 3 < elemInfo.length) {
                endIndex = elemInfo[i + 3] - 1;
            } else {
                endIndex = ordinates.length;
            }

            if (etype == 2 && interpretation == 1) {
                // Simple string
                List<Coordinate> lineCoords = extractCoordinates(ordinates, startIndex, endIndex);
                if (lineCoords.size() >= 2) {
                    lineStrings.add(
                            geometryFactory.createLineString(
                                    lineCoords.toArray(new Coordinate[0])));
                }
            } else if (etype == 6 && interpretation == 1) {
                // Composite string - Multi line set
                processCompoundLineString(ordinates, startIndex, endIndex, lineStrings);
            }
        }

        if (lineStrings.isEmpty()) {
            return geometryFactory.createMultiLineString(new LineString[0]);
        } else if (lineStrings.size() == 1) {
            return lineStrings.get(0);
        } else {
            return geometryFactory.createMultiLineString(lineStrings.toArray(new LineString[0]));
        }
    }

    /** Extract coordinates from ordinates array. */
    private void processCompoundLineString(
            double[] ordinates, int start, int end, List<LineString> lineStrings) {
        int currentIndex = start;

        while (currentIndex < end && currentIndex + 1 < ordinates.length) {
            // Find the beginning of the next line segment (when there is a significant change in X
            // or Y coordinates)
            List<Coordinate> segmentCoords = new ArrayList<>();
            segmentCoords.add(new Coordinate(ordinates[currentIndex], ordinates[currentIndex + 1]));

            int segmentEnd = findSegmentEnd(ordinates, currentIndex, end);

            // Extract the coordinates of the current line segment
            for (int j = currentIndex + 2; j < segmentEnd && j + 1 < ordinates.length; j += 2) {
                segmentCoords.add(new Coordinate(ordinates[j], ordinates[j + 1]));
            }

            if (segmentCoords.size() >= 2) {
                lineStrings.add(
                        geometryFactory.createLineString(segmentCoords.toArray(new Coordinate[0])));
            }

            currentIndex = segmentEnd;
        }
    }

    /** Extract coordinates from ordinates array. */
    private int findSegmentEnd(double[] ordinates, int start, int maxEnd) {
        // Simple heuristic method: When two consecutive points are far apart, it is considered as
        // the beginning of a new line segment
        for (int j = start + 4; j < maxEnd && j + 1 < ordinates.length; j += 2) {
            double dx = Math.abs(ordinates[j] - ordinates[j - 2]);
            double dy = Math.abs(ordinates[j + 1] - ordinates[j - 1]);
            if (dx > 5
                    || dy > 5) { // The threshold can be adjusted according to the actual situation
                return j;
            }
        }
        return maxEnd;
    }

    /** Create JTS polygon Geometry from OracleGeometry. */
    private Geometry createJTSPolygon(OracleGeometry oracleGeometry) {
        double[] ordinates = oracleGeometry.getOrdinates();
        if (ordinates == null || ordinates.length < 6) {
            throw new IllegalArgumentException("Incomplete polygon coordinate data");
        }

        List<Coordinate> shellCoords = new ArrayList<>();
        for (int i = 0; i < ordinates.length; i += 2) {
            shellCoords.add(new Coordinate(ordinates[i], ordinates[i + 1]));
        }
        LinearRing shell = geometryFactory.createLinearRing(shellCoords.toArray(new Coordinate[0]));
        return geometryFactory.createPolygon(shell);
    }

    /** Create JTS multiple polygon Geometry from OracleGeometry. */
    public MultiPolygon createJTSMultiPolygon(OracleGeometry oracleGeometry) {
        if (oracleGeometry == null) {
            return null;
        }

        int[] elemInfo = oracleGeometry.getElemInfo();
        double[] ordinates = oracleGeometry.getOrdinates();

        if (elemInfo == null || ordinates == null) {
            return null;
        }

        List<Polygon> polygons = new ArrayList<>();
        int currentIndex = 0;

        while (currentIndex < elemInfo.length) {
            int startingOffset = elemInfo[currentIndex];
            int elementType = elemInfo[currentIndex + 1];
            int interpretation = elemInfo[currentIndex + 2];

            // Processing polygon outer ring (1003) and inner ring (2003)
            if (elementType == 1003 || elementType == 2003) {
                // Calculate the starting position of coordinates in the coordinates array
                // Oracle's offset starts from 1 and needs to be converted to an array index
                // starting from 0
                int coordStartIndex = startingOffset - 1;

                // Calculate the number of coordinates for the current element
                int numPoints;
                if (interpretation == 1) {
                    // Simple polygon: Calculate the number of points based on the starting offset
                    // of the next element
                    if (currentIndex + 3 < elemInfo.length) {
                        int nextStartingOffset = elemInfo[currentIndex + 3];
                        numPoints = (nextStartingOffset - startingOffset) / 2;
                    } else {
                        // The last element, using the remaining coordinates
                        numPoints = (ordinates.length - coordStartIndex) / 2;
                    }
                } else {
                    // Composite polygon: Interpretation directly represents the number of points
                    numPoints = interpretation;
                }

                List<Coordinate> polyCoords = new ArrayList<>();

                // Extract coordinate data
                for (int i = 0; i < numPoints; i++) {
                    int coordIndex = coordStartIndex + i * 2;
                    if (coordIndex + 1 < ordinates.length) {
                        double x = ordinates[coordIndex];
                        double y = ordinates[coordIndex + 1];
                        polyCoords.add(new Coordinate(x, y));
                    }
                }

                // Verify polygon closure, JTS requires polygons to be closed
                if (polyCoords.size() >= 4) {
                    // Check if the beginning and end points are the same, and if they are
                    // different, add the beginning point to close it
                    if (!polyCoords.get(0).equals(polyCoords.get(polyCoords.size() - 1))) {
                        polyCoords.add(new Coordinate(polyCoords.get(0)));
                    }

                    // Create linear rings and polygons
                    if (polyCoords.size() >= 4) {
                        LinearRing ring =
                                geometryFactory.createLinearRing(
                                        polyCoords.toArray(new Coordinate[0]));

                        // For creating polygons with outer rings, the inner ring needs to be
                        // associated with the corresponding outer ring
                        if (elementType == 1003) {
                            Polygon polygon = geometryFactory.createPolygon(ring);
                            polygons.add(polygon);
                        }
                    }
                }
            }

            currentIndex += 3;
        }

        if (polygons.isEmpty()) {
            return null;
        }

        return geometryFactory.createMultiPolygon(polygons.toArray(new Polygon[0]));
    }

    /** Extract coordinates within a specified range from a coordinate array. */
    private List<Coordinate> extractCoordinates(double[] ordinates, int startIndex, int endIndex) {
        List<Coordinate> coordinates = new ArrayList<>();

        for (int i = startIndex; i < endIndex && i + 1 < ordinates.length; i += 2) {
            coordinates.add(new Coordinate(ordinates[i], ordinates[i + 1]));
        }

        return coordinates;
    }

    /** Create JTS GeometryCollection from OracleGeometry. */
    private Geometry createJTSGeometryCollection(OracleGeometry oracleGeometry) {
        int[] elemInfo = oracleGeometry.getElemInfo();
        double[] ordinates = oracleGeometry.getOrdinates();

        List<Geometry> geometries = new ArrayList<>();

        for (int i = 0; i < elemInfo.length; i += 3) {
            int startIndex = elemInfo[i] - 1;
            int etype = elemInfo[i + 1];
            int interpretation = elemInfo[i + 2];
            int endIndex;

            // Calculate the end position of the current element
            if (i + 3 < elemInfo.length) {
                endIndex = elemInfo[i + 3] - 1;
            } else {
                endIndex = ordinates.length;
            }
            OracleGeometry geometry = new OracleGeometry();
            double[] subCoordinates;
            switch (etype) {
                case 1: // point
                    if (interpretation == 1) {
                        subCoordinates = Arrays.copyOfRange(ordinates, startIndex, startIndex + 2);
                        geometry.setCoordinates(subCoordinates);
                        Geometry point = createJTSPoint(geometry);
                        if (point != null) {
                            geometries.add(point);
                        }
                    }
                    break;

                case 2: // line
                    if (interpretation == 1) {
                        subCoordinates = Arrays.copyOfRange(ordinates, startIndex, endIndex);
                        geometry.setOrdinates(subCoordinates);
                        Geometry line = createJTSLineString(geometry);
                        if (line != null) {
                            geometries.add(line);
                        }
                    }
                    break;

                case 1003: // Polygonal outer ring
                case 2003: // Polygonal Inner Ring
                    subCoordinates = Arrays.copyOfRange(ordinates, startIndex, endIndex);
                    geometry.setOrdinates(subCoordinates);
                    Geometry polygon = createJTSPolygon(geometry);
                    if (polygon != null) {
                        geometries.add(polygon);
                    }
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported geometry types: " + etype);
            }
        }

        return geometryFactory.createGeometryCollection(geometries.toArray(new Geometry[0]));
    }
}
