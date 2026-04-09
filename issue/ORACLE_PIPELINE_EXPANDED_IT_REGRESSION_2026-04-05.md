# Oracle Pipeline Expanded IT Regression 2026-04-05

## Scope

Expanded Oracle container regression on the pipeline connector side after the DATE/LONG focused validation passed.

Covered IT classes:

- OracleFullTypesITCase
- OracleMetadataAccessorITCase
- OraclePipelineRecordEmitterITCase

## First Batch Result

Initial targeted batch result:

- OracleFullTypesITCase: PASS
- OraclePipelineRecordEmitterITCase: PASS
- OracleMetadataAccessorITCase: FAIL

The failure was not an Oracle container/runtime problem. It was an outdated expected schema in the common-types metadata assertion.

Observed actual mapping in OracleMetadataAccessor common-types schema:

- ID -> INT
- VAL_NUM_VS / VAL_INT / VAL_INTEGER / VAL_SMALLINT / VAL_NUMBER_38_NO_SCALE / VAL_NUMBER_38_SCALE_0 -> DECIMAL(38, 0)
- VAL_NUMBER_1 -> BOOLEAN
- VAL_NUMBER_2 -> TINYINT
- VAL_NUMBER_4 -> SMALLINT
- VAL_NUMBER_9 -> INT
- VAL_NUMBER_18 -> BIGINT
- VAL_NUMBER_2_NEGATIVE_SCALE -> TINYINT
- VAL_NUMBER_4_NEGATIVE_SCALE -> SMALLINT
- VAL_NUMBER_9_NEGATIVE_SCALE -> INT
- VAL_NUMBER_18_NEGATIVE_SCALE -> BIGINT
- VAL_NUMBER_36_NEGATIVE_SCALE -> DECIMAL(38, 0)

## Fix Applied

Updated OracleMetadataAccessorITCase expected schema to match the current Oracle numeric mapping baseline already exercised by the other Oracle tests.

## Verification

Verified by re-running OracleMetadataAccessorITCase directly from the oracle pipeline connector module:

- Tests run: 2
- Failures: 0
- Errors: 0
- Skipped: 0

Previously validated unchanged classes in the same expanded batch remain:

- OracleFullTypesITCase: Tests run 1, Failures 0, Errors 0, Skipped 0
- OraclePipelineRecordEmitterITCase: Tests run 1, Failures 0, Errors 0, Skipped 0

## Conclusion

The expanded Oracle pipeline IT regression is green after refreshing the outdated metadata schema baseline. The next sensible expansion target is the heavier source-side Oracle IT suite, especially OracleSourceITCase and NewlyAddedTableITCase.