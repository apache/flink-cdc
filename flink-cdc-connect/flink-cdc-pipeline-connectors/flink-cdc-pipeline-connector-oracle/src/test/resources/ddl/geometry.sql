-- Copyright 2023 Ververica Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--   http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  debezium
-- ----------------------------------------------------------------------------------------------------------------
-- Table for testing geometric types

CREATE TABLE DEBEZIUM.MYLAKE
(
    FEATURE_ID NUMBER PRIMARY KEY,
    NAME       VARCHAR2(32),
    SHAPE      MDSYS.SDO_GEOMETRY
);
ALTER TABLE DEBEZIUM.mylake
    ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO user_sdo_geom_metadata
VALUES ('MYLAKE',
        'SHAPE',
        MDSYS.SDO_DIM_ARRAY(
                MDSYS.SDO_DIM_ELEMENT('X', 0, 100, 0.05),
                MDSYS.SDO_DIM_ELEMENT('Y', 0, 100, 0.05)
            ),
        4326);


CREATE INDEX mylake_idx ON DEBEZIUM.MYLAKE (shape) INDEXTYPE IS MDSYS.SPATIAL_INDEX_V2;

INSERT INTO DEBEZIUM.MYLAKE VALUES (1,
        'center',
        SDO_GEOMETRY('POINT(116.6 24.343)', 4326));


-- Insertion Point Type - SDO-GTYPE: 2001
INSERT INTO DEBEZIUM.MYLAKE VALUES (
                                       2, 'two-dimensional point',
                                       SDO_GEOMETRY(2001, 4326, SDO_POINT_TYPE(10, 5, NULL), NULL, NULL)
                                   );

-- Insert Line String - SDO-GTYPE: 2002
INSERT INTO DEBEZIUM.MYLAKE VALUES (
                                       3, 'straight line segment',
                                       SDO_GEOMETRY(2002, 4326, NULL,
                                                    SDO_ELEM_INFO_ARRAY(1, 2, 1),
                                                    SDO_ORDINATE_ARRAY(10, 10, 20, 20, 30, 30))
                                   );

-- Polyline Insertion - SDO-GTYPE: 2002
INSERT INTO DEBEZIUM.MYLAKE VALUES (
                                       4, 'polyline',
                                       SDO_GEOMETRY(2002, 4326, NULL,
                                                    SDO_ELEM_INFO_ARRAY(1, 2, 1),
                                                    SDO_ORDINATE_ARRAY(5, 5, 15, 10, 25, 5, 35, 10))
                                   );

-- Insert Simple Polygon - SDO-GTYPE: 2003
INSERT INTO DEBEZIUM.MYLAKE VALUES (
                                       5, 'rectangle',
                                       SDO_GEOMETRY(2003, 4326, NULL,
                                                    SDO_ELEM_INFO_ARRAY(1, 3, 1),
                                                    SDO_ORDINATE_ARRAY(0, 0, 10, 0, 10, 10, 0, 10, 0, 0))
                                   );

-- Insert Multi Point Set - SDO-GTYPE: 2005
INSERT INTO DEBEZIUM.MYLAKE VALUES (
                                       6, 'Multi-Point',
                                       SDO_GEOMETRY(2005, 4326, NULL,
                                                    SDO_ELEM_INFO_ARRAY(1, 1, 1, 3, 1, 1, 5, 1, 1),
                                                    SDO_ORDINATE_ARRAY(10, 10, 20, 20, 30, 30))
                                   );

-- Revised Multi line Set Insertion Statement - SDO-GTYPE: 2006
INSERT INTO DEBEZIUM.MYLAKE VALUES (
                                       7, 'Multi line collection',
                                       SDO_GEOMETRY(2006, 4326, NULL,
                                                    SDO_ELEM_INFO_ARRAY(1, 2, 1, 5, 2, 1),
                                                    SDO_ORDINATE_ARRAY(10, 10, 20, 10, 10, 20, 20, 20)
                                           ));



-- Insert Multi Polygon Set - SDO-GTYPE: 2007
INSERT INTO DEBEZIUM.MYLAKE VALUES (
                                       8, 'Multi-Polygon',
                                       SDO_GEOMETRY(2007, 4326, NULL,
                                                    SDO_ELEM_INFO_ARRAY(1, 1003, 1, 11, 1003, 1),
                                                    SDO_ORDINATE_ARRAY(0,0, 10,0, 10,10, 0,10, 0,0,15,15, 25,15, 25,25, 15,25, 15,15))
                                   );

-- Insert Composite Geometry Collection - SDO-GTYPE: 2004
INSERT INTO DEBEZIUM.MYLAKE VALUES (
                                       9, 'Geometry Collection',
                                       SDO_GEOMETRY(2004, 4326, NULL,
                                                    SDO_ELEM_INFO_ARRAY(1, 1, 1, 3, 2, 1, 7, 1003, 1),
                                                    SDO_ORDINATE_ARRAY(25, 25, 30, 30, 35, 25, 0, 0, 10, 0, 10, 10, 0, 10, 0, 0))
                                   );