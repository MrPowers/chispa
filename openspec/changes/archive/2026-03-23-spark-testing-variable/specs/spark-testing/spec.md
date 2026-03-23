## ADDED Requirements

### Requirement: spark fixture provides SparkSession with SPARK_TESTING set
The system SHALL provide a pytest fixture named `spark` that returns a SparkSession object with the `SPARK_TESTING` environment variable set to `1` before session creation.

#### Scenario: Fixture creates SparkSession with SPARK_TESTING
- **WHEN** a test requests the `spark` fixture
- **THEN** the system sets `os.environ["SPARK_TESTING"] = "1"` before creating the SparkSession
- **AND** the SparkSession is created with `SparkSession.builder.master("local").appName("chispa").getOrCreate()`

#### Scenario: Fixture has session scope
- **WHEN** multiple tests request the `spark` fixture
- **THEN** the same SparkSession instance is returned for all tests within the test session
- **AND** no additional SparkSessions are created

#### Scenario: Fixture is available to all tests
- **WHEN** any test file in the tests directory requests the `spark` fixture parameter
- **THEN** the fixture is automatically discovered and injected by pytest
