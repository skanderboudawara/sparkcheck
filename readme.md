[![ci](https://github.com/skanderboudawara/sparkcheck/actions/workflows/ci.yml/badge.svg)](https://github.com/skanderboudawara/sparkcheck/actions/workflows/ci.yml)[![docs](https://github.com/skanderboudawara/sparkcheck/actions/workflows/sphinx.yml/badge.svg)](https://github.com/skanderboudawara/sparkcheck/actions/workflows/sphinx.yml)

# SparkChecker

## Description

**SparkChecker** is a library designed to perform constraints checks on Spark DataFrames.
By specifying your predicates in a `.yaml` file, the library validates these predicates against your data pipelines during the build process.

The primary objective is to make the `.yaml` file as simple and readable as possible so that it can be easily understood and reviewed by stakeholders such as Business Owners or Product Owners.
Results can be displayed or logged depending on the parameters provided.

---

## Key Features

- **Readable YAML configuration**: Designed to be simple and accessible for non-technical stakeholders, such as Business or Product Owners.
- **Customizable validation**: Supports configurable strategies (`warn` or `fail`) for each predicate, allowing flexibility in handling data quality issues.
- **Flexible logging**: Provides multiple options to view results — print logs to the console, write to a file, or both.
- ** Flexible error message**: You can use your custom error message or the default ones.
- **Robust error handling**: Automatically raises errors for critical validation failures to ensure pipeline reliability.
- **Minimal dependencies**: Built with simplicity in mind, requiring only `PySpark` and `PyYAML`, with no additional bloat. This lightweight design is intended to remain unchanged.

---

## Usage

To use the `sparkchecker` function:

```python
from sparkchecker import sparkchecker

df.sparkchecker(
    path="path_to_the_yaml_file.yml",
    raise_error=True,              # Set to True to raise an error on failed checks
    print_log=True,                # Set to True to print logs to the console
    write_file=True,               # Set to True to write logs to a file
    file_path="path_to_log_file",  # Optional path for the log file
)
```

### Parameters
- `path`:
  Path to the `.yaml` file containing the list of predicates.
- `raise_error` (default: `False`):
  If set to True, raises a `SparkCheckerError` when any predicate with a "fail" strategy fails.
- `print_log` (default: `False`):
  If set to True, prints the status of each predicate in the console.
- `write_file` (default: `False`):
  If set to `True`, writes a log file to the specified path.
- `file_path` (optional):
  Specifies the directory or filename for the log file. By default, the log file is written next to the `.yaml` file.

### Example
Refer to the example folder for a prototype .yaml file and a demonstration of how to run SparkChecker against a DataFrame.

```yaml
count:
  - higher:
      than: 0
      strategy : "warn"
is_empty:
  is: False
  strategy: "fail"
has_columns:
  - passengers_name: string
  - passengers_age
checks:
  - is_military:
    - equal:
        to: False
        strategy: "fail"
        message: "There should be no military in civilian dataset"
```

```python
# Example Python Script
from sparkchecker import sparkchecker

df.sparkchecker(
    path="example.yaml",
    raise_error=True,
    print_log=True,
    write_file=True,
    file_path="logs/sparkchecker.log",
)
```


## Construct the YAML File

### Structure

A predicate is generally constructed as follows:

```yaml
<check_name>:
  - <operator>:
      than: <mandatory_value>  # Can be an integer, string, column name, date, etc.
      strategy: <warn/fail>    # Optional. Default is "fail" if not provided.
      message: <custom_message> # Optional. Default messages will be applied if not provided.
```

### Notes
- You can use the following operators to compare a value to something else: `lower`, `lower_or_equal`, `equal`, `different`, `higher`, `higher_or_equal`
- Additionally, the keys than`, `to` and `value`are interchangeable aliases, allowing you to prioritize readability in your YAML file.
- The `strategy` field determines how the predicate failure is handled:
    - `warn` : Logs a warning but does not interrupt the pipeline.
    - `fail`: Raises an error, halting the pipeline execution.
- The `message` field allows you to provide a custom error or warning message for better traceability and context.

---

### DataFrame Predicates
Below are examples of predicates you can use for validating Spark DataFrames:

#### 1. Count
Ensure the DataFrame has more than <number> rows:

```yaml
count:
  - higher:
      than: <number>
      strategy : "warn"
```

#### 2.  Partitions
Ensure the number of partitions in the DataFrame is greater than <number>:
```yaml
partitions:
  - higher:
      than: <number>
      strategy : "warn"
```

#### 3. Is Empty
Check whether the DataFrame is empty:
```yaml
is_empty:
    is: False
    strategy : "warn"
```

#### 4. Check DataFrame Schema

Validate whether the DataFrame contains specific columns and optionally check their data types.

```yaml
has_columns:
  - passengers_name: string  # Checks that the column exists and matches the specified type.
  - passengers_age           # Checks that the column exists, regardless of its type.
```

**Supported Data Types**

The following data types are supported for schema validation:

- `string`
- `integer`
- `long`
- `float`
- `double`
- `timestamp`
- `date`
- `boolean`
- `bytes`
- `decimal(<precision>, <scale>)`

---

### Column Predicates

Column predicates are always defined under the `checks` field, which can be stacked. These checks are performed specifically on columns, and an error will be raised if a specified column does not exist.

**Very important:**
> In case you want to test the columns against another column, the second column should be wrapped in backticks as follow
> ```"`column_name`"```

Below are examples of predicates you can use for validating the columns of Spark DataFrames:

#### 1. Is Null

Ensure the column is either null or not null:

```yaml
checks:
  - <column_name>:
      - is_null:
          is: False  # Specify whether the column should be null or not.
```

#### 2. Comparison

Compare the column value to a specified value using an operator:

```yaml
checks:
  - <column_name>:
      - higher:         # Can also use operators like lower, equal, different, etc.
          than: <value>
```

or

```yaml
checks:
  - <column_name>:
      - higher:         # Can also use operators like lower, equal, different, etc.
          than: "`<other_column_name>`"
```
#### 3. Regex Pattern

Check if the column value matches a regular expression pattern. You can test patterns using sites like [https://regex101.com](https://regex101.com). The parser will automatically escape the regex.

```yaml
checks:
  - <column_name>:
      - pattern:
          value: "<your_pattern>"  # Ensure the pattern is in raw string format (r"").
```

or

```yaml
checks:
  - <column_name>:
      - pattern:
          value: "`<column_name_containing_patterns>`"
```

#### 4. Is In an array

Check if the column value exists in a predefined array of values:

```yaml
checks:
  - <column_name>:
      - in:
          values: [<value1>, <value2>, <value3>, ...]  # List of allowed values.
```

or

```yaml
checks:
  - <column_name>:
      - in:
          values: ["`<column_name1`>", "`<column_name2>`", <value3>, ...]  # List of allowed values.
```

**Note**:

If you want to allow `None` values in the array, make sure to explicitly include `None` in the list. By default, if `None` is found in the column but isn't included in the array, it will trigger an error, depending on your defined strategy.

Example:

```yaml
checks:
  - <column_name>:
      - in:
          values: ["USA", "FR", None]  # Explicitly include None to allow it.
```
