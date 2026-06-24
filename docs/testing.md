# Testing Guide

- Unit tests use JUnit 5 and AssertJ.
- Integration tests use Testcontainers and the `*IT.java` naming convention.
- `mvn test` runs only unit tests because Surefire excludes integration-test suffixes.
- `mvn verify` will pick up integration tests through Failsafe unless `-DskipITs` is supplied.
