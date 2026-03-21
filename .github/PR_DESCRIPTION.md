# Add FLINK_VERSION suffix support for version management

## Motivation

When building and deploying CDC connectors for different Flink versions, it's helpful to have the Flink version embedded in the artifact version. This makes it easier to distinguish which Flink version a particular artifact is compatible with.

## Changes

- Added `tools/update_version.sh` script for standalone version updates
- Enhanced `tools/releasing/deploy_staging_jars.sh` to automatically update project version when `FLINK_VERSION` is set

## Usage

### Deploy with Flink version suffix

When deploying with a specific Flink version, the script will automatically update the project version:

```bash
FLINK_VERSION=1.20 ./tools/releasing/deploy_staging_jars.sh
```

**Version transformation example:**

| Current Version | FLINK_VERSION | New Version |
|-----------------|---------------|-------------|
| `3.6-SNAPSHOT`  | `1.20`        | `3.6-1.20`  |
| `3.6`           | `1.20`        | `3.6-1.20`  |
| `3.6-1.20`      | `1.20`        | `3.6-1.20` (skipped, already has suffix) |

### Standalone version update

If you only want to update the version without deploying:

```bash
FLINK_VERSION=1.20 ./tools/update_version.sh
```

## Implementation Details

- Uses `versions-maven-plugin` to update versions across all modules
- Removes `-SNAPSHOT` suffix (if present) before appending the Flink version
- Skips version update if the version already ends with `-${FLINK_VERSION}` to avoid duplicate suffixes
