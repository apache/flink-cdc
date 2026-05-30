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

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for non-blocking Doris existing-column type diagnostics. */
class DorisExistingColumnTypeAnalyzerTest {

    @Test
    void testFlagsLowConfidenceForFeVarcharWithoutPrecision() {
        Column desiredColumn = Column.physicalColumn("name", DataTypes.VARCHAR(192));
        org.apache.doris.flink.rest.models.Field existingField = dorisField("name", "VARCHAR");

        DorisExistingColumnTypeAnalyzer.ExistingColumnTypeAssessment assessment =
                DorisExistingColumnTypeAnalyzer.assess(desiredColumn, existingField);

        Assertions.assertThat(assessment.typeDefinitionDrift).isTrue();
        Assertions.assertThat(assessment.lowConfidencePhysicalMetadata).isTrue();
        Assertions.assertThat(assessment.capacityRisk).isFalse();
        Assertions.assertThat(assessment.familyMismatch).isFalse();
    }

    @Test
    void testFlagsStringCapacityRisk() {
        Column desiredColumn = Column.physicalColumn("name", DataTypes.VARCHAR(192));
        org.apache.doris.flink.rest.models.Field existingField = dorisVarcharField("name", 32);

        DorisExistingColumnTypeAnalyzer.ExistingColumnTypeAssessment assessment =
                DorisExistingColumnTypeAnalyzer.assess(desiredColumn, existingField);

        Assertions.assertThat(assessment.capacityRisk).isTrue();
        Assertions.assertThat(assessment.lowConfidencePhysicalMetadata).isFalse();
    }

    @Test
    void testDoesNotFlagCapacityRiskWhenDecimalMetadataHasLowConfidence() {
        Column desiredColumn = Column.physicalColumn("amount", DataTypes.DECIMAL(17, 7));
        org.apache.doris.flink.rest.models.Field existingField = dorisField("amount", "DECIMAL");

        DorisExistingColumnTypeAnalyzer.ExistingColumnTypeAssessment assessment =
                DorisExistingColumnTypeAnalyzer.assess(desiredColumn, existingField);

        Assertions.assertThat(assessment.lowConfidencePhysicalMetadata).isTrue();
        Assertions.assertThat(assessment.capacityRisk).isFalse();
    }

    @Test
    void testUsesDorisMappedStringLengthForCapacityRisk() {
        Column desiredColumn = Column.physicalColumn("name", DataTypes.VARCHAR(64));
        org.apache.doris.flink.rest.models.Field existingField = dorisVarcharField("name", 128);

        DorisExistingColumnTypeAnalyzer.ExistingColumnTypeAssessment assessment =
                DorisExistingColumnTypeAnalyzer.assess(desiredColumn, existingField);

        Assertions.assertThat(assessment.desiredType).isEqualTo("VARCHAR(192)");
        Assertions.assertThat(assessment.capacityRisk).isTrue();
    }

    @Test
    void testDoesNotFlagLowConfidenceWhenVarcharTypeHasLength() {
        Column desiredColumn = Column.physicalColumn("name", DataTypes.VARCHAR(64));
        org.apache.doris.flink.rest.models.Field existingField =
                new org.apache.doris.flink.rest.models.Field(
                        "name", "VARCHAR(192)", null, 0, 0, null);

        DorisExistingColumnTypeAnalyzer.ExistingColumnTypeAssessment assessment =
                DorisExistingColumnTypeAnalyzer.assess(desiredColumn, existingField);

        Assertions.assertThat(assessment.lowConfidencePhysicalMetadata).isFalse();
        Assertions.assertThat(assessment.capacityRisk).isFalse();
    }

    @Test
    void testFlagsDatetimeScaleCapacityRisk() {
        Column desiredColumn = Column.physicalColumn("create_time", DataTypes.TIMESTAMP(3));
        org.apache.doris.flink.rest.models.Field existingField =
                dorisDateTimeField("create_time", 0);

        DorisExistingColumnTypeAnalyzer.ExistingColumnTypeAssessment assessment =
                DorisExistingColumnTypeAnalyzer.assess(desiredColumn, existingField);

        Assertions.assertThat(assessment.typeDefinitionDrift).isTrue();
        Assertions.assertThat(assessment.capacityRisk).isTrue();
        Assertions.assertThat(assessment.familyMismatch).isFalse();
    }

    @Test
    void testParsesDatetimeScaleFromTypeDefinition() {
        Column desiredColumn = Column.physicalColumn("create_time", DataTypes.TIMESTAMP(3));
        org.apache.doris.flink.rest.models.Field existingField =
                new org.apache.doris.flink.rest.models.Field(
                        "create_time", "DATETIME(3)", null, 0, 0, null);

        DorisExistingColumnTypeAnalyzer.ExistingColumnTypeAssessment assessment =
                DorisExistingColumnTypeAnalyzer.assess(desiredColumn, existingField);

        Assertions.assertThat(assessment.capacityRisk).isFalse();
        Assertions.assertThat(assessment.familyMismatch).isFalse();
    }

    @Test
    void testFlagsDecimalCapacityRisk() {
        Column desiredColumn = Column.physicalColumn("amount", DataTypes.DECIMAL(17, 7));
        org.apache.doris.flink.rest.models.Field existingField = dorisDecimalField("amount", 16, 7);

        DorisExistingColumnTypeAnalyzer.ExistingColumnTypeAssessment assessment =
                DorisExistingColumnTypeAnalyzer.assess(desiredColumn, existingField);

        Assertions.assertThat(assessment.capacityRisk).isTrue();
        Assertions.assertThat(assessment.familyMismatch).isFalse();
    }

    @Test
    void testFlagsDecimalIntegerCapacityRisk() {
        Column desiredColumn = Column.physicalColumn("amount", DataTypes.DECIMAL(10, 2));
        org.apache.doris.flink.rest.models.Field existingField = dorisDecimalField("amount", 10, 5);

        DorisExistingColumnTypeAnalyzer.ExistingColumnTypeAssessment assessment =
                DorisExistingColumnTypeAnalyzer.assess(desiredColumn, existingField);

        Assertions.assertThat(assessment.capacityRisk).isTrue();
        Assertions.assertThat(assessment.familyMismatch).isFalse();
    }

    @Test
    void testParsesDecimalCapacityFromTypeDefinition() {
        Column desiredColumn = Column.physicalColumn("amount", DataTypes.DECIMAL(17, 7));
        org.apache.doris.flink.rest.models.Field existingField =
                new org.apache.doris.flink.rest.models.Field(
                        "amount", "DECIMAL(16,7)", null, 0, 0, null);

        DorisExistingColumnTypeAnalyzer.ExistingColumnTypeAssessment assessment =
                DorisExistingColumnTypeAnalyzer.assess(desiredColumn, existingField);

        Assertions.assertThat(assessment.lowConfidencePhysicalMetadata).isFalse();
        Assertions.assertThat(assessment.capacityRisk).isTrue();
    }

    @Test
    void testFlagsIntegerFamilyMismatch() {
        Column desiredColumn = Column.physicalColumn("id", DataTypes.BIGINT());
        org.apache.doris.flink.rest.models.Field existingField = dorisField("id", "INT");

        DorisExistingColumnTypeAnalyzer.ExistingColumnTypeAssessment assessment =
                DorisExistingColumnTypeAnalyzer.assess(desiredColumn, existingField);

        Assertions.assertThat(assessment.familyMismatch).isTrue();
        Assertions.assertThat(assessment.capacityRisk).isFalse();
    }

    @Test
    void testFlagsCharVarcharFamilyMismatch() {
        Column desiredColumn = Column.physicalColumn("name", DataTypes.VARCHAR(17));
        org.apache.doris.flink.rest.models.Field existingField = dorisCharField("name", 51);

        DorisExistingColumnTypeAnalyzer.ExistingColumnTypeAssessment assessment =
                DorisExistingColumnTypeAnalyzer.assess(desiredColumn, existingField);

        Assertions.assertThat(assessment.familyMismatch).isTrue();
        Assertions.assertThat(assessment.capacityRisk).isFalse();
    }

    @Test
    void testTreatsDorisTinyintZeroPrecisionAsBooleanMetadataAlias() {
        Column desiredColumn = Column.physicalColumn("deleted", DataTypes.BOOLEAN());
        org.apache.doris.flink.rest.models.Field existingField = dorisTinyintField("deleted", 0);

        DorisExistingColumnTypeAnalyzer.ExistingColumnTypeAssessment assessment =
                DorisExistingColumnTypeAnalyzer.assess(desiredColumn, existingField);

        Assertions.assertThat(assessment.existingType).isEqualTo("TINYINT");
        Assertions.assertThat(assessment.typeDefinitionDrift).isTrue();
        Assertions.assertThat(assessment.familyMismatch).isFalse();
    }

    @Test
    void testFlagsBooleanTinyintWithPrecisionAsFamilyMismatch() {
        Column desiredColumn = Column.physicalColumn("deleted", DataTypes.BOOLEAN());
        org.apache.doris.flink.rest.models.Field existingField = dorisTinyintField("deleted", 1);

        DorisExistingColumnTypeAnalyzer.ExistingColumnTypeAssessment assessment =
                DorisExistingColumnTypeAnalyzer.assess(desiredColumn, existingField);

        Assertions.assertThat(assessment.typeDefinitionDrift).isTrue();
        Assertions.assertThat(assessment.familyMismatch).isTrue();
    }

    @Test
    void testFlagsTinyintDesiredAgainstPhysicalBooleanAsFamilyMismatch() {
        Column desiredColumn = Column.physicalColumn("mode", DataTypes.TINYINT());
        org.apache.doris.flink.rest.models.Field existingField = dorisField("mode", "BOOLEAN");

        DorisExistingColumnTypeAnalyzer.ExistingColumnTypeAssessment assessment =
                DorisExistingColumnTypeAnalyzer.assess(desiredColumn, existingField);

        Assertions.assertThat(assessment.typeDefinitionDrift).isTrue();
        Assertions.assertThat(assessment.familyMismatch).isTrue();
    }

    @Test
    void testDoesNotFlagTinyintDesiredAgainstPhysicalTinyintWithoutPrecision() {
        Column desiredColumn = Column.physicalColumn("mode", DataTypes.TINYINT());
        org.apache.doris.flink.rest.models.Field existingField = dorisTinyintField("mode", 0);

        DorisExistingColumnTypeAnalyzer.ExistingColumnTypeAssessment assessment =
                DorisExistingColumnTypeAnalyzer.assess(desiredColumn, existingField);

        Assertions.assertThat(assessment.typeDefinitionDrift).isFalse();
        Assertions.assertThat(assessment.familyMismatch).isFalse();
    }

    @Test
    void testDoesNotFlagSufficientCapacity() {
        Column desiredColumn = Column.physicalColumn("name", DataTypes.VARCHAR(64));
        org.apache.doris.flink.rest.models.Field existingField = dorisVarcharField("name", 192);

        DorisExistingColumnTypeAnalyzer.ExistingColumnTypeAssessment assessment =
                DorisExistingColumnTypeAnalyzer.assess(desiredColumn, existingField);

        Assertions.assertThat(assessment.capacityRisk).isFalse();
        Assertions.assertThat(assessment.familyMismatch).isFalse();
    }

    private static org.apache.doris.flink.rest.models.Field dorisField(String name, String type) {
        return new org.apache.doris.flink.rest.models.Field(name, type, null, 0, 0, null);
    }

    private static org.apache.doris.flink.rest.models.Field dorisVarcharField(
            String name, int precision) {
        return new org.apache.doris.flink.rest.models.Field(
                name, "VARCHAR", null, precision, 0, null);
    }

    private static org.apache.doris.flink.rest.models.Field dorisCharField(
            String name, int precision) {
        return new org.apache.doris.flink.rest.models.Field(name, "CHAR", null, precision, 0, null);
    }

    private static org.apache.doris.flink.rest.models.Field dorisTinyintField(
            String name, int precision) {
        return new org.apache.doris.flink.rest.models.Field(
                name, "TINYINT", null, precision, 0, null);
    }

    private static org.apache.doris.flink.rest.models.Field dorisDecimalField(
            String name, int precision, int scale) {
        return new org.apache.doris.flink.rest.models.Field(
                name, "DECIMAL", null, precision, scale, null);
    }

    private static org.apache.doris.flink.rest.models.Field dorisDateTimeField(
            String name, int scale) {
        return new org.apache.doris.flink.rest.models.Field(name, "DATETIME", null, 0, scale, null);
    }
}
