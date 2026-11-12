# SAS-to-PySpark-migration

This repository holds a Jupyter notebook with a Python class (`SASPySparkMigration`) that maps the SAS procedures and DATA-step patterns in the table below (all but `PROC ARIMA`, whose method is a place-holder) to PySpark DataFrame operations.

Each public method of the `SASPySparkMigration` class has a docstring that names the SAS procedure or DATA-step pattern that the method replaces. Every docstring except `summarize_dataset`'s shows the SAS code. A SAS program that uses only the procedures and DATA-step patterns in the table below (except `PROC ARIMA`) and only the options that each docstring shows can therefore be mapped procedure by procedure once the fixes to the docstrings in ROADMAP.md bring each docstring's SAS code in line with its method and with SAS. ROADMAP.md also lists where the methods' results still differ from SAS.

Once the fixes in ROADMAP.md bring each method in line with SAS's defaults, the class can also serve as a reference for checking code produced by SAS-to-PySpark converters.

## Who it is for

The notebook is for analysts and data engineers who are moving SAS code to Apache Spark and want a PySpark equivalent for each procedure that they use.

## What it covers

| SAS | Method |
|---|---|
| `PROC SORT` | `sort_data` |
| `PROC MEANS` | `calculate_means` |
| `PROC FREQ` | `frequency_analysis` |
| `PROC UNIVARIATE` | `univariate_analysis` |
| `PROC TABULATE` | `cross_tabulation` |
| `PROC RANK` | `rank_data` |
| `PROC FORMAT` with `PUT` | `format_values` |
| `PROC TRANSPOSE` | `transpose_data` |
| DATA-step `MERGE` | `merge_datasets` |
| `PROC SQL` | `sql_query` |
| `PROC CORR` | `correlation_analysis` |
| `PROC STDIZE` | `standardize_data` and `handle_missing` |
| `PROC EXPAND` moving average | `rolling_window_analysis` |
| DATA-step `IF` / `ELSE IF` | `conditional_column_creation` |
| `PROC CONTENTS` | `summarize_dataset` |
| `PROC SURVEYSELECT` | `split_dataset` |
| `PROC ARIMA` | `time_series_forecasting` (place-holder, not implemented yet) |

## Built with

- Python.
- PySpark (DataFrame API, window functions, `pyspark.ml.feature`, `pyspark.ml.stat`).
- pandas and NumPy.
- `unittest`.

## How to run

The code is in 1 Jupyter notebook (`jk_SAS_to_PySpark_migration_0036.ipynb`).

1. Install Java 17 or 21 (Spark needs a JVM, and PySpark 4.3.0 is built for Java 17). The Spark 3.5.3 docs list only Java 8/11/17, while the 13 tests passed on Java 21 with PySpark 3.5.3 and with 4.3.0. On Java 25, PySpark 3.5.3 stopped before its 1st test with "getSubject is not supported", while 4.3.0 passed the 13 tests.

2. On Python 3.14.6, PySpark 3.5.3 stopped before its 1st test with a `PicklingError`, while 4.3.0 passed the 13 tests. On Python 3.8 to 3.13, install the Python packages (on Python 3.13, the 13 tests passed with PySpark 3.5.3 and with 4.3.0):

   ```
   pip install "pyspark==3.5.3" "pandas<3" numpy pytest setuptools notebook
   ```

   - `setuptools` is needed on Python 3.12 and later because `pyspark.ml` and PySpark's pandas conversion in 3.5.x import `distutils`.
   - pandas stays below 3 because, in the notebook's tests, pandas 3 makes PySpark 3.5.3 print `Pandas4Warning` warnings ("The copy keyword is deprecated") that pandas 2 does not.
   - With pandas 2.3.3 (which `pandas<3` gives on Python 3.9 to 3.13) on PySpark 3.5.3, all 13 tests pass when cell-5 runs them, but the run still prints 3 kinds of warning:
     - The `is_datetime64tz_dtype` deprecation (ROADMAP.md has the details).
     - 2 `distutils` deprecations from PySpark's own `pyspark/sql/pandas/utils.py`.
     - 12 `ResourceWarning` warnings for unclosed sockets (in a local run).
   - On Python 3.8, `pandas<3` gives pandas 2.0.3, and the run printed only the 2nd and 3rd kinds.
   - `notebook` installs Jupyter into the same environment so that its kernel sees these packages.
   - Step-1 and step-2 are not needed in Google Colab:
     - Colab's published apt list has Java 21 (`openjdk-21-jdk-headless`) installed.
     - Colab's published package list gives PySpark 4.0.5 and pandas 2.2.3, and the 13 tests passed on a local copy of those versions.
     - Cell-1's own `!pip install` line adds any package that is missing.

   On Python 3.14, install PySpark 4.3.0 instead (on Python 3.14.6, the 13 tests passed with PySpark 4.3.0, pyarrow 26.0.1, pandas 2.3.3 from `pandas<3`):

   ```
   pip install "pyspark==4.3.0" "pandas<3" "pyarrow==26.0.1" numpy pytest notebook
   ```

   - pandas stays below 3 here too. With pandas 3.1.1, the same run printed 4 `FutureWarning` warnings ("PySpark does not yet fully support pandas >= 3.0.0.") and 6 `Pandas4Warning` warnings, while pandas 2.3.3 gave none of either.

3. Open the notebook in Jupyter or Google Colab.

4. Run all cells.

The 5th cell runs the 18 unit tests. The 6th cell prints the 17 worked examples, then runs the tests again.

The sample data in the notebook is invented for testing and holds no real person's data.

## Planned work

Planned work is listed in [ROADMAP.md](ROADMAP.md).

## Licence

The code is under the MIT licence (see [LICENSE](LICENSE)).
