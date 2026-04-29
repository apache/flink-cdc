<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Flink CDC AI Agent Instructions

This file provides guidance for AI coding agents working with the Apache Flink CDC.

## Prerequisites

- Java 11 (baseline; all code must compile and run on Java 11) and Java 17
- Maven 3.8.6 or higher
- Git
- Unix-like environment (Linux, macOS, WSL)
- Docker (required for running integration tests and e2e tests)

## Commands

### Build

- Fast dev build (skip tests and format checks): `mvn clean install -DskipTests`
- Full build against Flink 1.x (default): `mvn clean package -DskipTests`
- Full build against Flink 2.x: `mvn clean package -DskipTests -Pflink2`
- Single module (e.g. `flink-cdc-common`): `mvn clean package -DskipTests -pl flink-cdc-common -am`

### Testing

- Run all tests for a module: `mvn verify -pl flink-cdc-common`
- Run all tests against Flink 2.x: `mvn verify -pl flink-cdc-common -Pflink2`
- Run a single test class: `mvn -pl flink-cdc-common -Dtest=MyTest test`
- Run a single test method: `mvn -pl flink-cdc-common -Dtest=MyTest#myMethod test`

### Code Quality

- Before committing changes, run `mvn spotless:apply` and `mvn spotless:check` to enforce code style rules.
- Make sure newly added files have proper ASF license headers.

## Repository Structure

Flink CDC is organized around the Pipeline abstraction: a user-defined data pipeline that reads from one or more sources, optionally transforms records, and writes to one or more sinks. The core modules implement this abstraction; the connector modules provide the concrete source and sink implementations.

### Core Modules

- `flink-cdc-common` — Shared API and data model used across all modules: CDC event types (`DataChangeEvent`, `SchemaChangeEvent`, etc.), the schema model, data types, source/sink interfaces, `Factory` SPI, route definitions, UDF interfaces, and utility classes. Most new abstractions start here.
- `flink-cdc-runtime` — Runtime implementation of the Pipeline: operators for reading, routing, transforming (expression evaluation via Calcite + Janino), and writing CDC events.
- `flink-cdc-composer` — Pipeline assembly and deployment layer. Translates a `PipelineDefinition` into a runnable Flink job, wiring sources, operators, and sinks together. Supports Flink-native, Kubernetes, and YARN deployment.
- `flink-cdc-cli` — Command-line entry point (`flink-cdc.sh`). Parses YAML pipeline definitions and delegates to `flink-cdc-composer`.
- `flink-cdc-dist` — Distribution packaging. Produces the `flink-cdc-<version>-bin` release archive.

### Flink Version Compatibility

Currently, Flink CDC supports two Flink generations simultaneously:

- `flink-cdc-flink1-compat` — Flink 1.x compatibility layer (currently 1.20.3). Default profile.
- `flink-cdc-flink2-compat` — Flink 2.x compatibility layer (currently 2.2.0). Activated via `-Pflink2`.

All modules that depend on Flink APIs must declare their Flink dependencies as `provided` and reference `${flink.version}`, which is resolved by the active profile.
Please verify your changes in both Flink 1.20 (LTS) and Flink 2.x (latest).

### Connectors (`flink-cdc-connect/`)

Connectors are split into two categories:

- Source Connectors (`flink-cdc-connect/flink-cdc-source-connectors/`) are CDC sources for DataStream and Flink SQL jobs.
- Pipeline Connectors (`flink-cdc-connect/flink-cdc-pipeline-connectors/`) are Pipeline connectors for the YAML API.

### Tests

Write unit tests and integration tests in the corresponding submodules. End-to-end tests are located in these modules:

- `flink-cdc-e2e-tests/` — End-to-end tests parent module.
  - `flink-cdc-e2e-utils` — Shared test utilities (container management, assertions)
  - `flink-cdc-source-e2e-tests` — E2E tests for source connectors
  - `flink-cdc-pipeline-e2e-tests` — E2E tests for pipeline connectors

### Documentation

- `docs/` — Hugo-based documentation site. `docs/content/` for English, `docs/content.zh/` for Chinese. Update both when adding new features.

## Coding Standards

- **Format Java files with Spotless before every commit:** `mvn spotless:apply`.
- **Import order** (enforced by Checkstyle): `org.apache.flink.cdc` → `org.apache.flink` → other third-party → `javax` → `java`. Static imports go last. No star imports.
- **Forbidden imports** (enforced by Checkstyle):
  - JUnit 4 (`org.junit.*` except `org.junit.jupiter.*`) — use JUnit 5 Jupiter instead
  - `org.junit.jupiter.api.Assertions` and `org.hamcrest` — use AssertJ instead
  - `com.google.common.*` — use `flink-shaded-guava` instead
  - `com.google.common.base.Preconditions` — use Flink CDC's `Preconditions`
  - `com.google.common.annotations.VisibleForTesting` — use `@VisibleForTesting` from `org.apache.flink.cdc.common.annotation`
- **API stability annotations:** Every user-facing class and method must carry one of the annotations from `org.apache.flink.cdc.common.annotation`:
  - `@Public` — stable across major versions
  - `@PublicEvolving` — may change in minor versions
  - `@Experimental` — may change at any time
  - `@Internal` — no stability guarantee; users should not depend on it
- **Logging:** Use SLF4J with parameterized placeholders (`LOG.info("foo {}", bar)`), never string concatenation.
- **Braces:** Always use braces for `if`/`else`/`for`/`while`/`do` blocks.
- **Comments:** All comments should be written in English, and keep it concise. Only write comments when necessary. Avoid trivial `@param`, `@return` comments in JavaDocs.
- Apache License 2.0 header required on all new files.

## Testing Standards

- Use **JUnit 5** (`org.junit.jupiter`) and **AssertJ** (`org.assertj.core.api.Assertions`) for all new tests. Do not use JUnit 4 or Hamcrest.
- Test classes are **package-private** (no `public` modifier on the class).
- **Unit tests** are named `*Test.java` (e.g., `SchemaUtilsTest`).
- **Integration tests** (requiring Docker / real databases) are named `*ITCase.java` (e.g., `MySqlSourceITCase`).
- Add tests for new behavior, covering success, failure, and edge cases. Tests should be self-explanatory; avoid verbose comments in test classes.
- For bug fixes, verify the new test fails without the fix before confirming it passes with it.

## Commits and PRs

### Commit message format

- `[FLINK-XXXX][component] Description` where `FLINK-XXXX` is the JIRA issue number and `component` is the affected area (e.g. `connect/mysql`, `pipeline-connector/kafka`, `docs`, `runtime`)
- `[hotfix][component] ...` or `[docs][component] ...` for trivial fixes without a JIRA issue
- `[ci] Description` for CI-only changes

### Pull request conventions

- Title format mirrors the commit message: `[FLINK-XXXX][component] Title`
- A corresponding JIRA issue is required (except trivial changes)
- Fill out the PR template completely: describe purpose, change log, testing approach, and documentation impact
- Ensure CI passes before requesting review
- Enable GitHub Actions on your fork before opening a PR
- When AI tools were used, check the AI disclosure checkbox and uncomment the `Generated-by` line in the PR template, per [ASF Generative Tooling Guidance](https://www.apache.org/legal/generative-tooling.html)

## Boundaries

### Ask first

- Adding or changing `@Public` or `@PublicEvolving` annotations (user-facing API commitments)
- New dependencies
- Large cross-module refactors
- Changes to serialization formats or checkpoint behavior
- Changes to hot paths that could impact performance (per-record processing, state access)

### Never

- Commit secrets, credentials, or tokens
- Push directly to `apache/flink-cdc`; always work from your fork
- Mix unrelated changes into one PR
- Use JUnit 4 or Hamcrest in new test code
- Use `org.junit.jupiter.api.Assertions`; use AssertJ instead
- Add `Co-Authored-By` with an AI agent in commit messages; use `Generated-by: <Tool Name and Version>` instead