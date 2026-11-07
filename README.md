# SAS-to-PySpark-migration

This is a Python library that maps common SAS procedures and DATA-step patterns to PySpark DataFrame operations.

Each public method of the `SASPySparkMigration` class names, in its docstring, the SAS procedure or DATA-step pattern that it replaces. Every docstring except `summarize_dataset`'s shows the SAS code. A SAS program can therefore be ported procedure by procedure.

Once the fixes in ROADMAP.md bring each method in line with SAS's defaults, the library can also serve as a reference for checking code produced by SAS-to-PySpark converters.

## Who it is for

It is for analysts and data engineers who are moving SAS code to Apache Spark and want a PySpark equivalent for each procedure they use.

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

The code is in a single Jupyter notebook (`jk_SAS_to_PySpark_migration_0036.ipynb`).

1. Install Java (Spark needs a JVM).

2. Install the Python packages. The 13 tests pass on PySpark 3.5.3 and on 4.3.0:

   ```
   pip install "pyspark==3.5.3" pandas numpy pytest setuptools
   ```

   `setuptools` is needed on Python 3.12 and later because `pyspark.ml` and PySpark's pandas conversion in 3.5.x import `distutils`.

3. Open the notebook in Jupyter or Google Colab. In cell-1, move the line `%%capture` above the comment line since a cell magic must be the 1st line of its cell. Then run all cells.

The 5th cell runs the 13 unit tests. The 6th cell prints worked examples, then runs the tests again.

The sample data in the notebook is invented for testing. It holds no real person's data.

## Planned work

Planned work is listed in [ROADMAP.md](ROADMAP.md).

## License

MIT (see [LICENSE](LICENSE)).
