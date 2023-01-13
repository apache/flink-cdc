# Changelog JSON Format

**WARNING:** The CDC format `changelog-json` is deprecated since Flink CDC version 2.2.
The CDC format `changelog-json` was introduced at the point that Flink didn't offer any CDC format. Currently, Flink offers several well-maintained CDC formats i.e.[Debezium CDC, MAXWELL CDC, CANAL CDC](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/connectors/table/formats/overview/), we recommend user to use above CDC formats.

### Compatibility Note

User can still obtain and use the deprecated `changelog-json` format from older Flink CDC version e.g. [flink-format-changelog-json-2.1.1.jar](https://repo1.maven.org/maven2/com/ververica/flink-format-changelog-json/2.1.1/flink-format-changelog-json-2.1.1-SNAPSHOT.jar).
