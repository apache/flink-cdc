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
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.connectors.doris.utils.DorisTypeUtils;

import org.apache.doris.flink.rest.models.Field;

import java.util.Locale;
import java.util.OptionalInt;

/** Analyzes existing Doris column types for non-blocking existing-table diagnostics. */
final class DorisExistingColumnTypeAnalyzer {

    private DorisExistingColumnTypeAnalyzer() {}

    static ExistingColumnTypeAssessment assess(Column desiredColumn, Field existingField) {
        String desiredType =
                DorisTypeUtils.normalizeDorisTypeDefinition(
                        DorisTypeUtils.toDorisTypeString(desiredColumn.getType()));
        String existingType =
                DorisTypeUtils.normalizeDorisTypeDefinition(
                        DorisTypeUtils.buildExistingFieldType(existingField));
        String desiredBase = parseTypeBase(desiredType);
        String existingBase = parseTypeBase(existingType);

        return new ExistingColumnTypeAssessment(
                desiredType,
                existingType,
                !desiredType.equals(existingType),
                hasLowConfidencePhysicalMetadata(existingField),
                hasCapacityRisk(
                        desiredColumn.getType(),
                        existingField,
                        desiredType,
                        desiredBase,
                        existingBase),
                hasFamilyMismatch(existingField, desiredBase, existingBase));
    }

    private static boolean hasLowConfidencePhysicalMetadata(Field existingField) {
        String baseType = parseTypeBase(existingField.getType());
        switch (baseType) {
            case "VARCHAR":
            case "CHAR":
                return resolvePhysicalStringLength(existingField) <= 0;
            case "DECIMAL":
            case "DECIMALV2":
            case "DECIMALV3":
                return resolvePhysicalDecimalPrecision(existingField) <= 0;
            default:
                return false;
        }
    }

    private static boolean hasCapacityRisk(
            DataType desiredType,
            Field existingField,
            String desiredTypeDefinition,
            String desiredBase,
            String existingBase) {
        if (!hasReliableCapacityMetadata(existingField)) {
            return false;
        }
        if (hasStringCapacityRisk(
                desiredTypeDefinition, existingField, desiredBase, existingBase)) {
            return true;
        }
        if (hasDecimalCapacityRisk(desiredType, existingField, desiredBase)) {
            return true;
        }
        return hasDatetimeCapacityRisk(desiredType, existingField, desiredBase, existingBase);
    }

    private static boolean hasReliableCapacityMetadata(Field existingField) {
        String baseType = parseTypeBase(existingField.getType());
        switch (baseType) {
            case "VARCHAR":
            case "CHAR":
                return resolvePhysicalStringLength(existingField) > 0;
            case "DECIMAL":
            case "DECIMALV2":
            case "DECIMALV3":
                return resolvePhysicalDecimalPrecision(existingField) > 0
                        && resolvePhysicalDecimalScale(existingField) >= 0;
            default:
                return true;
        }
    }

    private static boolean hasStringCapacityRisk(
            String desiredTypeDefinition,
            Field existingField,
            String desiredBase,
            String existingBase) {
        if (!isStringBase(desiredBase) || !isStringBase(existingBase)) {
            return false;
        }
        int desiredLength = parseTypeParameter(desiredTypeDefinition, 0);
        if (desiredLength <= 0 || desiredLength >= VarCharType.MAX_LENGTH) {
            return false;
        }
        int physicalLength = resolvePhysicalStringLength(existingField);
        if (physicalLength <= 0) {
            return false;
        }
        return desiredLength > physicalLength;
    }

    private static boolean hasDecimalCapacityRisk(
            DataType desiredType, Field existingField, String desiredBase) {
        if (!"DECIMAL".equals(desiredBase)
                || !isDecimalBase(parseTypeBase(existingField.getType()))) {
            return false;
        }
        OptionalInt desiredPrecision = DataTypes.getPrecision(desiredType);
        if (!desiredPrecision.isPresent()) {
            return false;
        }
        int desiredScale = DataTypes.getScale(desiredType).orElse(0);
        int physicalPrecision = resolvePhysicalDecimalPrecision(existingField);
        int physicalScale = resolvePhysicalDecimalScale(existingField);
        if (physicalPrecision <= 0 || physicalScale < 0) {
            return false;
        }
        int desiredIntegerDigits = desiredPrecision.getAsInt() - desiredScale;
        int physicalIntegerDigits = physicalPrecision - physicalScale;
        return desiredScale > physicalScale || desiredIntegerDigits > physicalIntegerDigits;
    }

    private static boolean hasDatetimeCapacityRisk(
            DataType desiredType, Field existingField, String desiredBase, String existingBase) {
        if (!isTemporalBase(desiredBase) || !isTemporalBase(existingBase)) {
            return false;
        }
        OptionalInt desiredScale = DataTypes.getPrecision(desiredType);
        if (!desiredScale.isPresent()) {
            return false;
        }
        int physicalScale = resolvePhysicalDatetimeScale(existingField);
        return desiredScale.getAsInt() > physicalScale;
    }

    private static boolean hasFamilyMismatch(
            Field existingField, String desiredBase, String existingBase) {
        if (desiredBase.equals(existingBase)) {
            return false;
        }
        if (isBooleanTinyintCompatible(desiredBase, existingBase, existingField)) {
            return false;
        }
        if (isCharVarcharFamilyMismatch(desiredBase, existingBase)) {
            return true;
        }
        if (isIntegerFamilyMismatch(desiredBase, existingBase)) {
            return true;
        }
        if (isBooleanTinyintFamilyMismatch(existingField, desiredBase, existingBase)) {
            return true;
        }
        return !resolveTypeFamily(desiredBase).equals(resolveTypeFamily(existingBase));
    }

    private static boolean isBooleanTinyintCompatible(
            String desiredBase, String existingBase, Field existingField) {
        return "BOOLEAN".equals(desiredBase)
                && "TINYINT".equals(existingBase)
                && existingField.getPrecision() == 0;
    }

    private static boolean isCharVarcharFamilyMismatch(String desiredBase, String existingBase) {
        return isCharBase(desiredBase) != isCharBase(existingBase)
                && isStringBase(desiredBase)
                && isStringBase(existingBase);
    }

    private static boolean isIntegerFamilyMismatch(String desiredBase, String existingBase) {
        return isIntegerBase(desiredBase)
                && isIntegerBase(existingBase)
                && !desiredBase.equals(existingBase);
    }

    private static boolean isBooleanTinyintFamilyMismatch(
            Field existingField, String desiredBase, String existingBase) {
        if ("BOOLEAN".equals(desiredBase) && "TINYINT".equals(existingBase)) {
            return existingField.getPrecision() > 0;
        }
        return "BOOLEAN".equals(existingBase) && "TINYINT".equals(desiredBase);
    }

    private static int resolvePhysicalStringLength(Field existingField) {
        int parsedLength = parseTypeParameter(existingField.getType(), 0);
        if (parsedLength > 0) {
            return parsedLength;
        }
        if (existingField.getPrecision() > 0) {
            return existingField.getPrecision();
        }
        return -1;
    }

    private static int resolvePhysicalDecimalPrecision(Field existingField) {
        int parsedPrecision = parseTypeParameter(existingField.getType(), 0);
        if (parsedPrecision > 0) {
            return parsedPrecision;
        }
        return existingField.getPrecision() > 0 ? existingField.getPrecision() : -1;
    }

    private static int resolvePhysicalDecimalScale(Field existingField) {
        int parsedScale = parseTypeParameter(existingField.getType(), 1);
        if (parsedScale >= 0) {
            return parsedScale;
        }
        return resolvePhysicalDecimalPrecision(existingField) > 0
                ? Math.max(existingField.getScale(), 0)
                : -1;
    }

    private static int resolvePhysicalDatetimeScale(Field existingField) {
        int parsedScale = parseTypeParameter(existingField.getType(), 0);
        if (parsedScale >= 0) {
            return parsedScale;
        }
        return Math.max(existingField.getScale(), 0);
    }

    private static int parseTypeParameter(String typeDefinition, int parameterIndex) {
        if (typeDefinition == null) {
            return -1;
        }
        int open = typeDefinition.indexOf('(');
        int close = typeDefinition.indexOf(')');
        if (open < 0 || close <= open) {
            return -1;
        }
        String[] parameters = typeDefinition.substring(open + 1, close).split(",");
        if (parameterIndex >= parameters.length) {
            return -1;
        }
        try {
            return Integer.parseInt(parameters[parameterIndex].trim());
        } catch (NumberFormatException ignored) {
            return -1;
        }
    }

    private static String parseTypeBase(String typeDefinition) {
        if (typeDefinition == null || typeDefinition.trim().isEmpty()) {
            return "";
        }
        String normalized = typeDefinition.trim().toUpperCase(Locale.ROOT);
        int parameterStart = normalized.indexOf('(');
        return parameterStart < 0 ? normalized : normalized.substring(0, parameterStart).trim();
    }

    private static String resolveTypeFamily(String baseType) {
        if (isStringBase(baseType)) {
            return "STRING";
        }
        if (isIntegerBase(baseType)) {
            return "INTEGER";
        }
        if (isTemporalBase(baseType)) {
            return "TEMPORAL";
        }
        if ("BOOLEAN".equals(baseType)) {
            return "BOOLEAN";
        }
        if ("DECIMAL".equals(baseType) || isDecimalBase(baseType)) {
            return "DECIMAL";
        }
        if ("FLOAT".equals(baseType) || "DOUBLE".equals(baseType)) {
            return "FLOATING";
        }
        return "UNKNOWN";
    }

    private static boolean isStringBase(String baseType) {
        return "CHAR".equals(baseType) || "VARCHAR".equals(baseType) || "STRING".equals(baseType);
    }

    private static boolean isCharBase(String baseType) {
        return "CHAR".equals(baseType);
    }

    private static boolean isIntegerBase(String baseType) {
        return "TINYINT".equals(baseType)
                || "SMALLINT".equals(baseType)
                || "INT".equals(baseType)
                || "INTEGER".equals(baseType)
                || "BIGINT".equals(baseType);
    }

    private static boolean isTemporalBase(String baseType) {
        return "DATE".equals(baseType)
                || "DATETIME".equals(baseType)
                || "TIMESTAMP".equals(baseType);
    }

    private static boolean isDecimalBase(String baseType) {
        return "DECIMAL".equals(baseType)
                || "DECIMALV2".equals(baseType)
                || "DECIMALV3".equals(baseType);
    }

    static final class ExistingColumnTypeAssessment {
        final String desiredType;
        final String existingType;
        final boolean typeDefinitionDrift;
        final boolean lowConfidencePhysicalMetadata;
        final boolean capacityRisk;
        final boolean familyMismatch;

        private ExistingColumnTypeAssessment(
                String desiredType,
                String existingType,
                boolean typeDefinitionDrift,
                boolean lowConfidencePhysicalMetadata,
                boolean capacityRisk,
                boolean familyMismatch) {
            this.desiredType = desiredType;
            this.existingType = existingType;
            this.typeDefinitionDrift = typeDefinitionDrift;
            this.lowConfidencePhysicalMetadata = lowConfidencePhysicalMetadata;
            this.capacityRisk = capacityRisk;
            this.familyMismatch = familyMismatch;
        }
    }
}
