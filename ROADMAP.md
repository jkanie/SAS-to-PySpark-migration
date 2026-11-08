# Planned work

Each "PySpark 3.5.3 run" below used PySpark 3.5.3 with pandas 3.1.1 on Python 3.13. Each "4.3.0" result used PySpark 4.3.0 with pyarrow 26.0.1 on the same Python and pandas.

## Fixes

- `conditional_column_creation` writes the quote characters into the result. Worked example-14 produces `'Adult'` and `'Senior'` (quotes included) because each value is passed to `when` as a Python string literal. The method should parse each value as a SQL expression (with `F.expr`) so that the result holds the value without its quotes. In PySpark 3.5.3 and 4.3.0 runs, `F.expr("'Senior'")` gave `Senior`, while the method's plain string gave `'Senior'`. Passing plain values works only if example-14 drops its inner quotes (`"Senior"` in place of `"'Senior'"`).

- `standardize_data` fails on a constant column because the standard deviation (or the range) is 0. In a PySpark 3.5.3 run, the method returned `NULL` for every row. On 4.3.0, it stopped with `DIVIDE_BY_ZERO`. On a 1-row column, the standard deviation is NULL rather than 0, and the default `zscore` returned NULL on both versions without an error. SAS's PROC STDIZE says "Constant variables are not standardized.". The method should therefore keep a constant column's values as they are. Its constant-column check should catch a NULL standard deviation too so that a 1-row column keeps its value.

- `sql_query` and `univariate_analysis` use the notebook's global `spark` session. They should use the DataFrame's own session (`self.df.sparkSession`) so that the class works outside the notebook.

- `rolling_window_analysis`:
  - `rolling_window_analysis` builds its window with `Window.orderBy()` and no column. The row order behind each moving average is therefore not defined. The method needs an order-by column parameter (like `ID` in `PROC EXPAND`). Because the row order is not defined, the method should sort by that column, while PROC EXPAND only checks the order that it reads: "PROC EXPAND verifies that the ID values are nonmissing and in ascending order.". On input already in ID order, the method and PROC EXPAND give the same rows.
  - In a PySpark 3.5.3 run ordered by a column with a null, the null row sorted 1st, then entered the average. PROC EXPAND checks that each ID value is non-missing (the check quoted above): "An error message is produced and the observation is ignored when an invalid ID value is found in the input data set.". The method should therefore drop such rows and log an error message for each.

- `split_dataset` is annotated as `-> (pyspark.sql.DataFrame, pyspark.sql.DataFrame)`, which is a tuple of classes, not a valid type-hint. The method returns the list from `randomSplit`. The hint should therefore be `list[pyspark.sql.DataFrame]`. The other route is to unpack the 2 parts and return them as a tuple annotated `tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame]` (mypy 2.4.1 rejected `return tuple(...)` with that hint since mypy types the result of `tuple(...)` as `tuple[DataFrame, ...]`). Both hints use the built-in forms because ruff's UP006 flags `List[...]` and `Tuple[...]` (see the `ruff` item below).

- `frequency_analysis`, `calculate_means`, `cross_tabulation`, `rank_data`, `correlation_analysis` do not follow SAS's defaults for missing values. Each should follow the SAS default:
  - PROC FREQ leaves a missing value out of its table and out of the percentages ("Also by default, PROC FREQ does not include observations with missing values in the computation of percentages and statistics."), while its `OUT=` data-set keeps 1 row with the missing frequency ("The TABLES statement OUT= data set includes an observation that contains the missing value frequency."). Today, `frequency_analysis` gives the null row a percentage and counts it in the total (on a, a, null, b, it gave level-a 50.0, where SAS gives 66.67). The method should keep the null row's count and give it no percentage. The method should take the percentages over the non-missing rows.
  - PROC MEANS drops a row whose `CLASS` value is missing. Today, `calculate_means` keeps a null group.
  - PROC TABULATE also drops a row whose `CLASS` value is missing. Today, `cross_tabulation` adds a `null` row plus a `null` column.
  - PROC RANK leaves a missing value unranked. Today, `rank_data` ranks a null 1st.
  - PROC CORR uses pairwise deletion. Today, `correlation_analysis` drops every row that has a null in any of the columns.

  `frequency_analysis`, `calculate_means`, `cross_tabulation` also need an option for SAS's `MISSING` rule so that code that uses `MISSING` can count a missing value as a level.

  `correlation_analysis` needs an option for PROC CORR's `NOMISS` rule so that code that uses `NOMISS` gets its rule ("NOMISS excludes observations with missing values from the analysis.").

- NaN, all-blank strings, infinity:
  - In a float or double column, a NaN counts as a number, not as missing. In a PySpark 3.5.3 run, `spark.createDataFrame` kept the NaN of a pandas DataFrame (on 4.3.0, Arrow turned it into NULL). A CSV value `NaN` that was read as a double stayed NaN on both versions. 12 methods took NaN as a number in PySpark 3.5.3 runs on the values 1.0, 3.0, NaN:
    - `calculate_means` gave a mean of NaN from a count of 3.
    - `frequency_analysis` counted NaN as a level.
    - `rank_data` ranked NaN last.
    - `sort_data` put NaN after 3.0.
    - `standardize_data` gave NaN in every row.
    - `univariate_analysis` gave NaN for the mean, the standard deviation, the skewness, the kurtosis, the maximum.
    - `rolling_window_analysis` gave NaN for the window that held NaN.
    - `cross_tabulation` and `transpose_data` each made a column `NaN`.
    - `merge_datasets` matched a NaN key with a NaN key.
    - `sql_query` counted NaN in `WHERE v > 2`.
    - `conditional_column_creation` treated the condition `v > 2` as true for NaN, where SAS's missing value is smaller than any number.

    By contrast, `handle_missing` filled NaN as missing, and `correlation_analysis` dropped the row that held NaN.

    SAS has no NaN -- a numeric value is a number or a missing value. SAS says "PROC MEANS excludes missing values for the analysis variables before calculating statistics.". The class should turn NaN into null when it reads a DataFrame.
  - A blank string is missing in SAS as well: "A character expression is considered missing if it evaluates to a string that contains all blanks or has a length of zero.". In PySpark 3.5.3 runs, an empty string and a string of 2 blanks made 2 levels in `frequency_analysis`, 2 groups in `calculate_means`, 2 columns in each of `cross_tabulation` and `transpose_data`. In `sql_query` (on the values a, an empty string, null), `WHERE s IS NULL` found 1 row, where SAS finds 2. When the class reads a DataFrame (as the NaN item above asks), it should also turn an all-blank string into null.
  - Infinity also passes as a number. On inf, 1, 2, null, `calculate_means` gave a mean of inf. `handle_missing` filled the null with inf. None of the SAS pages that were searched says what SAS stores for infinity. The class should therefore raise an error for it so that no method returns a result that no SAS page backs.

- String, date, time-stamp, boolean columns:
  - In a PySpark 3.5.3 run, `calculate_means`, `standardize_data` (with its default `zscore`), `rank_data`, `rolling_window_analysis` took a string column without an error:
    - On the values x1, x2, 10, a group holding x1 and x2 got a NULL mean, a count of 2, a minimum of x1 (as text) from `calculate_means`.
    - With 10 in the group that holds x1 and x2, `calculate_means` gave a mean of 10.0 (x1 and x2 dropped without a warning).
    - `standardize_data` gave NULL in every row.
    - `rank_data` ranked the values as text.
    - `rolling_window_analysis` gave NULL for the rows holding x1 and x2, then 10.0 for the row holding 10.

    With `method='minmax'`, `standardize_data` stopped on both versions with "unsupported operand type(s) for -: 'str' and 'str'".

    On 4.3.0, `calculate_means`, `standardize_data`, `rolling_window_analysis` stopped with `CAST_INVALID_INPUT`, while `rank_data` still ranked the values as text. On a string column that holds only digits (10 and 9), `calculate_means`, `standardize_data`, `rolling_window_analysis` ran on both versions without an error. `calculate_means` then gave a mean of 9.5 with a minimum of 10 and a maximum of 9 (compared as text).

    PROC STDIZE says "The VAR statement lists numeric variables to be standardized.". PROC RANK "Computes the ranks for one or more numeric variables.". PROC MEANS and PROC EXPAND pick only numeric variables when no variable is named: "If you omit the VAR statement, then PROC MEANS analyzes all numeric variables that are not listed in the other statements." and "If no CONVERT statement is used, all the numeric variables in the input data set except those appearing in the BY and ID statements are processed.". Each method should raise an error for a column that is not numeric.
  - Dates and time-stamps are not handled the same way in every method. SAS keeps a date as "the number of days between January 1, 1960, and a specified date.". In PySpark 3.5.3 runs, `calculate_means`, `standardize_data`, `univariate_analysis`, `handle_missing`, `correlation_analysis`, `rolling_window_analysis` stopped on a date column. `rolling_window_analysis` averaged a time-stamp as Unix seconds. In a session on America/Whitehorse time, midnight on January 1, 1960, became -315586800.0 (-315619200.0 in UTC), where SAS gives 0. The class should turn dates and time-stamps into SAS date and datetime values or refuse them with 1 clear error.
  - Booleans are not handled the same way in every method either:
    - `correlation_analysis` took booleans as 0 and 1.
    - `rank_data` ranked False before True.
    - `calculate_means`, `standardize_data`, `univariate_analysis`, `handle_missing`, `rolling_window_analysis` refused them.

    The class should pick 1 rule for booleans (either take them as 0 and 1 or refuse them with 1 clear error) and apply it in every method so that a boolean column gives the same behaviour in every method.

- `univariate_analysis`:
  - `univariate_analysis` returns Spark's population skewness and kurtosis. PROC UNIVARIATE defaults to `VARDEF=DF`. The method should use the `VARDEF=DF` forms.
  - Its percentiles come from `approxQuantile` at a relative error of 0.01. PROC UNIVARIATE defaults to exact percentiles by definition-5. On the sample's ages, PROC UNIVARIATE therefore gives a median of 53.5, where the method returns 52. The method should use exact definition-5 percentiles.
  - The method also returns no N, which heads the Moments table of PROC UNIVARIATE's default output. The method should add N.
  - SAS's list of ODS tables marks BasicMeasures, ExtremeObs, Moments, Quantiles, TestsForLocation as default. Those 5 tables include these statistics:
    - The mode.
    - The variance.
    - The range.
    - The interquartile range.
    - The standard error of the mean.
    - The 5 lowest and 5 highest observations.
    - The tests for location.

    Apart from `min` and `max` (the 1st of the 5 lowest and the last of the 5 highest, which SAS lists in ascending order), none of the 15 statistics that the method returns is in that list. The method should add the statistics of that list. Otherwise, its docstring should name the tables that the method leaves out so that a user knows which default output the method lacks.
  - Its result also passes through pandas, which turns a statistic that Spark leaves NULL into NaN. On 1 row, a PySpark 3.5.3 run gave NaN for the standard deviation, the skewness, the kurtosis. On a constant column (5.0 in 3 rows), a PySpark 3.5.3 run gave NaN for the skewness and the kurtosis. A 4.3.0 run (with Arrow on) kept those statistics NULL. SAS has no NaN. The method should build its result in Spark.
  - The default Moments table also holds these statistics:
    - "Sum Weights".
    - "Sum Observations".
    - "Uncorrected SS".
    - "Corrected SS".
    - "Coeff Variation".

    The method returns none of them. It should add them.
  - SAS prints the MissingValues table when missing values exist -- its table list marks it "Default, if missing values exist". The method does not return that table and should add it.
  - SAS also leaves skewness missing below 3 non-missing values and kurtosis below 4 (n in the quotes below is the count of non-missing values). For skewness, SAS says "If VARDEF=DF, then n must be greater than 2.". For kurtosis, SAS says "If VARDEF=DF, then n must be greater than 3.". In a PySpark 3.5.3 run on the 2 values 1.0 and 3.0, the method gave a skewness of 0.0 and a kurtosis of -2.0. The method should return NULL below those counts.
  - Its output-types also differ by version -- `min` and `max` of an integer column came back as `bigint` on 3.5.3 and `int` on 4.3.0 because 4.3.0 turns Arrow on for pandas. A result built in Spark (as the pandas item above asks) keeps 1 type on both versions.

- 14 docstrings disagree with their methods or with SAS in 19 places:
  - The `cross_tabulation` example builds a `DataFrameAnalyzer` (a class that does not exist). The example should build a `SASPySparkMigration`.
  - The `cross_tabulation` SAS code uses `/ PTOC`, which is not a `TABLE` statement option. The column percentages that `normalize=True` adds are PROC TABULATE's `COLPCTN`. The SAS code should show that statistic.
  - The `frequency_analysis` SAS code (`TABLES col1 col2`) asks for 2 tables of 1 variable each, while the method returns 1 joint table, as `TABLES col1*col2 / LIST` does. The SAS code should be `TABLES col1*col2 / LIST NOCUM` (with `NOPERCENT` added for `include_pct=False`) because `LIST` shows cumulative frequencies and percentages unless `NOCUM` is given: "NOCUM suppresses the display of cumulative frequencies and percentages in one-way frequency tables and in list-format crosstabulation tables".
  - The `frequency_analysis` SAS code also sorts by value (SAS's default `ORDER=INTERNAL`), while the method sorts by count, as `ORDER=FREQ` does. The method should therefore sort by value unless asked for `ORDER=FREQ`.
  - The `format_values` SAS code writes a new variable (`formatted_col`), while the method over-writes the column that it formats. The 1st `format_values` item below (the new string column) makes the method match its SAS code.
  - The `split_dataset` SAS code keeps only the sampled rows, while the method returns both parts. Returning both parts needs `OUTALL` and its `Selected` flag in SAS. The SAS code should show `OUTALL`.
  - The `split_dataset` SAS code also has no `SEED=`. Without `SEED=`, PROC SURVEYSELECT "uses the time of day from the computer’s clock to obtain the initial seed.". Each run of that SAS code therefore draws a new sample, while the method's seed of 42 gives the same sample each time that the same DataFrame (with the same partitions) is split. SAS re-produces a sample from "this same seed value in the SEED= option, along with the same sample selection parameters.". The SAS code should show `SEED=42`.
  - The `rolling_window_analysis` SAS code leaves out `METHOD=`. PROC EXPAND would therefore fill inner missing values with its default `METHOD=SPLINE`, while the method does not interpolate them (a 2-row moving average over 1.0, null, 2.0 gave 1.0, 1.0, 2.0 in a PySpark 3.5.3 run). The SAS code should show `METHOD=NONE`.
  - The `summarize_dataset` docstring names only PROC CONTENTS (which "Prints descriptions of the contents of one or more SAS library files"), while the method also prints 5 data rows and value-counts. The docstring should name PROC PRINT and PROC FREQ for those parts.
  - The `conditional_column_creation` SAS code assigns character values with no `LENGTH` statement. For a variable made from a character literal, SAS takes the "Length of first literal encountered". With the values of worked example-14, SAS therefore stores `Minor`, `Adult`, `Senio`, where the method (once the quote-character fix of the 1st `conditional_column_creation` item is in) gives `Senior`. The SAS code should start with a `LENGTH` statement that sets the variable's length to that of the longest value.
  - The `sort_data` SAS code has no `OUT=`. "Without the OUT= option, PROC SORT replaces the original data set with the sorted observations", while the method returns a new DataFrame. The SAS code should show `OUT=`.
  - The `handle_missing` SAS code has no `OUT=` either: "If you omit the OUT= option, PROC STDIZE creates an output data set named according to the DATAn convention.". The method instead returns a new DataFrame that its caller names. The SAS code should show `OUT=`.
  - The `handle_missing` SAS code shows only `METHOD=MEAN`, while the method also takes `median` and `mode` (PROC STDIZE has no `mode`, as the method-names item under `handle_missing` says). The SAS code should show `METHOD=MEDIAN` beside `METHOD=MEAN`, and the docstring should say that `mode` has no PROC STDIZE match so that a user does not look for one.
  - The `rolling_window_analysis` SAS code shows only `MOVAVE`, while the method also gives `MOVSUM`, `MOVMIN`, `MOVMAX`. The SAS code should show `MOVSUM`, `MOVMIN`, `MOVMAX` too.
  - The `correlation_analysis` SAS code shows only `PEARSON`, while the method also takes `spearman`. The SAS code should show `SPEARMAN` too.
  - The `sql_query` SAS code reads `FROM dataset`, while the method registers the data as `temp_table`. In a PySpark 3.5.3 run, `FROM dataset` stopped with `TABLE_OR_VIEW_NOT_FOUND`. The SAS code and example-9 should use the view-name of the plain-bugs `sql_query` item.
  - The `rank_data`, `transpose_data`, `merge_datasets` SAS code uses `BY` with no `PROC SORT` before it. For a procedure's BY statement, SAS says "either the observations in the data set must be sorted by all the variables that you specify, or they must be indexed appropriately.". For `MERGE`, SAS says "The data sets that are listed in the MERGE statement must be sorted in order of the values of the variables that are listed in the BY statement, or they must have an appropriate index.". The sample data is not sorted by `diagnosis` (the BY variable of `test_rank_data` and worked example-5). The SAS code of each of the 3 docstrings should start with a `PROC SORT` step.

- `rank_data`:
  - `rank_data` has no rank for `TIES=MEAN` (SAS's default, which gives tied values the mean of their ranks), `TIES=HIGH`, `TIES=LOW`. Its default should be `TIES=MEAN` (as in PROC RANK). The method should also take `TIES=HIGH` and `TIES=LOW` so that code that uses them can be ported (the `PERCENT` item below needs `TIES=HIGH`). Its docstring should then show `TIES=MEAN` (or no `TIES=`).
  - Its `method='percent'` uses `percent_rank`, which gives a value from 0 to 1 by (rank - 1) / (n - 1) (where rank is the value's rank and n is the number of rows in the window). PROC RANK's `PERCENT` divides each rank by the count of non-missing values and multiplies by 100 (with `TIES=HIGH` as its default there). As a result, PROC RANK gives 4 values the ranks 25, 50, 75, 100, where the method gives 0, 0.33, 0.67, 1. The method should compute rank × 100 / n (where rank is the value's rank under `TIES=HIGH` and n is the count of non-missing values).
  - Its `method='row_number'` gave 2 tied values the ranks 1 and 2 in a PySpark 3.5.3 run. PROC RANK's 4 `TIES=` values (`HIGH`, `LOW`, `MEAN`, `DENSE`) each give tied values 1 shared rank. The option should therefore go. Otherwise, the method's docstring should say that this option has no PROC RANK match so that a user does not take its ranks for SAS's.
  - The method also has no option for `GROUPS=`, which "assigns group values ranging from 0 to number-of-groups minus 1" by `FLOOR(rank*k/(n+1))` (where rank is the value's rank, k is the number of groups, n is the count of non-missing values). Spark's `ntile` does not match it. On 1, 1, 1, 1, 2, 3, 4, 5, 6, 7 with 4 groups, a PySpark 3.5.3 run of `ntile` put the 4 tied 1s in 2 groups, where the formula puts them all in group-0. The method should add a groups option with PROC RANK's formula.
  - It also ranks only from smallest to largest. PROC RANK's `DESCENDING` option "reverses the direction of the ranks.". The largest value then gets rank-1. The method should take a descending option.
  - It also returns the rows in a new order. In a PySpark 3.5.3 run on the ids 1 to 5 (with the values 5, 3, 1, 4, 2), the ids came back as 3, 5, 2, 4, 1 (in rank order). The `OUT=` listing in SAS's own PROC RANK example keeps the input order, with the ranks unsorted beside it. The method should restore the input order.

- `merge_datasets`:
  - `merge_datasets` joins the 2 sides. A key that appears 2 times on each side therefore gives 4 rows. SAS says "A DATA step that performs a many-to-many merge does not produce a Cartesian product across a BY-group.". The method should warn when both sides repeat a key. Its docstring should say that the method matches `MERGE` only for 1-to-1 and 1-to-many keys so that a user with repeated keys on both sides knows that the result differs.
  - When both sides hold a non-key column of the same name, the join keeps both columns. Selecting that column then fails with `AMBIGUOUS_REFERENCE`, as a PySpark 3.5.3 run showed. A DATA-step `MERGE` keeps 1 column. Where a key matches 1-to-1, the column's value comes from the data-set listed last. The method should also keep 1 column and, for a 1-to-1 key, take its value from the right side (the data-set listed last).
  - Its default (`how='inner'`) also drops a key found on 1 side only. In a PySpark 3.5.3 run, key-1 and key-2 on the left with key-2 and key-3 on the right gave only key-2. A `MERGE` with no sub-setting `IF` keeps all 3 -- for a BY group that a data-set lacks, "the program data vector contains missing values for the variables that are unique to that data set.". The default should be `how='outer'`.
  - When a key is an integer on 1 side and a string on the other, Spark casts the key, then joins. In a PySpark 3.5.3 run, key-1 matched "1" as well as "01". Key-2 matched " 2". A match-merging paper on SAS's own site says "Each BY variable must be of the same type (numeric or character) in all the input data sets.". SAS stops with an error that the variable "has been defined as both character and numeric.". The method should raise an error when the key-types differ.
  - A null key also never matches a null key. In a PySpark 3.5.3 run, an outer join gave 2 rows for the 2 null keys. SAS forms a BY group from "the set of all observations with the same value for the BY variable". PROC SQL pairs missing keys (as the PROC SQL join item under `sql_query` below says). None of the SAS pages that were searched says outright that a `MERGE` pairs 2 missing BY values. Matching null keys with `eqNullSafe` would pair them as PROC SQL does. Because none of the SAS pages that were searched confirms that a `MERGE` pairs them, that change waits on a test checked against a SAS run.
  - The join also moves the key columns to the front. In a PySpark 3.5.3 run, the left columns (name, id, score) with the right columns (id and extra) gave the order id, name, score, extra. A paper on SAS's site says that the DATA step builds its variables "in the order in which they are encountered by the compiler.". `MERGE a b` therefore keeps the order of `a`, then adds the new columns of `b`. The method should also keep the column order of the left side, then add the new columns of the right side.

- `sort_data`:
  - `sort_data` has no tie-break. For rows with identical BY values, PROC SORT's default `EQUALS` "maintains the relative order of the observations within the input data set", while Spark's `sort` documentation makes no promise about tied rows. The method should add the input order as a last sort-key.
  - When `ascending` is a list shorter than `columns`, `zip` drops the extra columns from the sort without a warning. In a PySpark 3.5.3 run on the rows (b, 9), (a, 1), (b, 3), `sort_data(["g", "v"], [False])` left `v` out of the sort and gave 9 before 3 within group-b. PROC SORT sorts by every BY variable. The method should raise an error when the 2 lists differ in length.

- `transpose_data`:
  - `transpose_data` keeps 1 value without a warning when an ID value repeats within a BY group, as a PySpark 3.5.3 run showed. PROC TRANSPOSE stops with "ERROR: The ID value xxxxxxxx occurs twice in the same BY group." unless `LET` is given. `LET` keeps the last value. The method should raise an error by default, with a `let` option that keeps the last value.
  - PROC TRANSPOSE also adds a `_NAME_` column that names the transposed variable. The method should add that column too.
  - For a numeric ID variable, PROC TRANSPOSE "changes the formatted ID value into a valid SAS name.". The PROC TRANSPOSE `ID` statement page gives 3 rules:
    - "When the first character of the formatted value is numeric, the procedure prefixes an underscore to the value". The 2 ID values 1 and 2 therefore become `_1` and `_2`, where the method gave `1` and `2`.
    - "Remaining invalid characters are replaced by underscores.".
    - For a value that looks like a number, "PROC TRANSPOSE changes the characters +, -, and . to P, N, and D, respectively.". In a PySpark 3.5.3 run, the 2 ID values 1.5 and -2.0 became the columns `1.5` and `-2.0`.

    The method should name numeric ID columns the same way.
  - For a character ID, the default `VALIDVARNAME=V7` rule applies: "A variable name cannot contain blanks or special characters except for the underscore.". The PROC TRANSPOSE `ID` statement page adds that "invalid characters will be replaced with underscores". On the sample data, a PySpark 3.5.3 run named a column `Heart Disease`, where SAS gives `Heart_Disease`. The `ID` statement sentence quoted above goes on: "or, if the name appears to be a numeric constant, an underscore will be used as a prefix and the characters +, -, and . will be changed to P, N, and D.". By that rule, a character ID made of digits gets an under-score pre-fix. The `StudentID` value 0545 (of the column-order item below) becomes `_0545`, where PySpark 3.5.3 and 4.3.0 runs of the method gave `0545`. The method should apply both rules. The 1st rule puts an under-score in place of each invalid character. The 2nd rule adds an under-score pre-fix to a name that looks like a number and puts P, N, D in place of `+`, `-`, `.`. In PySpark 3.5.3 and 4.3.0 runs, the character IDs 1.5, -2, +3 stayed `1.5`, `-2`, `+3`, where the 2nd rule gives `_1D5`, `_N2`, `_P3`.
  - Names in SAS also ignore case: "For example, cat, Cat, and CAT all represent the same variable.". In a PySpark 3.5.3 run, the 2 ID values a and A became 2 columns. Selecting `a` then stopped with `AMBIGUOUS_REFERENCE`. The method should treat ID values that differ only in case as 1 ID value, with the duplicate-ID error above.
  - The default `VALIDVARNAME=V7` rule also says "The length of a SAS variable names can be up to 32 characters.". The PROC TRANSPOSE `ID` statement page says "The procedure truncates to 32 characters any ID value that is longer than 32 characters" (under its heading for numeric ID values) and notes that "The character substitutions and truncation required to ensure that the variable name formed from the ID variables combined with the PREFIX, DELIMITER, and SUFFIX option values can cause duplicate output data set variable names". In a PySpark 3.5.3 run, 2 character ID values that share their 1st 32 characters became 2 columns (each over 32 characters long). The method should cut names to 32 characters before its duplicate-ID check.
  - With more than 10,000 distinct ID values, a PySpark 3.5.3 run stopped with "The pivot column id has more than 10000 distinct values". Passing the ID values to `pivot` avoids that limit, as a 2nd run with 10,001 values showed. The method should pass them.
  - In a PySpark 3.5.3 run, a row with a null ID value became a column named `null`. PROC TRANSPOSE "does not transpose observations that have a missing value for one or more ID variables" and writes a warning to the log. The method should drop those rows with a warning.
  - In a PySpark 3.5.3 run, the binary ID values `b"\xff"` and `b"\xfe"` became 2 columns that both print as "�" and hold NULL. The method therefore lost both values without an error (4.3.0 kept them, under 2 columns that both print as "�"). Passing the ID values to `pivot` gave NULL on 3.5.3 too. The method should either raise an error for a binary ID column or pivot on a text form of each value (such as `hex`) so that no value is lost.
  - `transpose_data` sorts its new columns by ID value, while PROC TRANSPOSE keeps the order in which each ID value 1st appears. In SAS's PROC TRANSPOSE example-1 data (`score`), the `StudentID` values come in the order 0545, 1252, 1167, 1230, 2527, 4860, 0674. In SAS's PROC TRANSPOSE example-3 (Labeling Transposed Variables), PROC CONTENTS gives `sn0674` position-8 and `sn4860` position-7. SAS therefore kept that input order. In PySpark 3.5.3 and 4.3.0 runs on the same 7 values, the method returned the columns in the order 0545, 0674, 1167, 1230, 1252, 2527, 4860. The method should pass the ID values to `pivot` in the order of their 1st row so that the columns come out as SAS writes them.

- `split_dataset`:
  - `split_dataset` uses `randomSplit`, whose part sizes vary -- on 10 rows at a ratio of 0.8, the seeds 0 to 49 gave a 1st part of 5 to 10 rows. PROC SURVEYSELECT turns `SAMPRATE` into an exact sample size (the row-count times the rate, rounded up). The method should draw that exact count.
  - PROC SURVEYSELECT also reads a rate from 1 to 100 as a percentage and "treats the value 1 as 100% instead of 1%.". In a PySpark 3.5.3 run, `split_dataset(80)` stopped with `VALUE_NOT_POSITIVE`. `split_dataset(1)` stopped with a `ClassCastException`. The method should take a rate both as a fraction below 1 and as a percentage from 1 to 100.
  - `split_dataset(0)` stopped with the same `ClassCastException` because the weights are integers. The rate check of the `split_dataset(0.0)` item below stops a rate of 0 with a clear error. The method should still cast the weights to float so that a rate of 1 (read as 100%, as the percentage item above says) does not give integer weights.
  - `split_dataset(0.0)` gave an empty part, while SURVEYSELECT says "The sampling rate value must be a positive number.". The method should raise an error for a rate of 0 or less.
  - In a PySpark 3.5.3 run, the same 20 rows in 1, 2, 4 partitions gave 3 different samples under seed-42. SAS re-produces a sample from the same seed and the same parameters (as the `split_dataset` docstring item quotes). The method should draw in a way that does not depend on the partitions.
  - A seed of 0 or below still gave a fixed split, while SURVEYSELECT reads such a seed like no seed -- it "uses the time of day from the computer’s clock to obtain an initial seed.". The docstring should state that difference so that a user who ports code with a seed of 0 or below knows that the method still gives a fixed split.
  - `split_dataset` also returns each part in a new order because `randomSplit` sorts each partition by all columns before it draws. In PySpark 3.5.3 and 4.3.0 runs of `split_dataset(0.7, seed=1)` on 1 partition, the ids 6, 5, 4, 3, 2, 1 (in that input order) came back as 1, 2, 3, 4 in the 1st part and 5, 6 in the 2nd. SURVEYSELECT keeps the input order: "By default for most sample selection methods, the order of the sample observations in the OUT= data set corresponds to the order in the DATA= input data set.". The method should keep the input order in each part (for example, by numbering the rows with `monotonically_increasing_id` before the split and sorting each part by that number). With that change, a run on 3 partitions gave each part in the input order on both versions.

- `standardize_data`, `handle_missing`, `rolling_window_analysis` add new columns, where SAS replaces the input column:
  - `standardize_data` and `handle_missing` add new `_standardized` and `_imputed` columns. PROC STDIZE's `OUT=` data-set is by default "a copy of the DATA= data set except that the analyzed variables have been standardized" (under the names of the input variables). A standardised column gets a pre-fix only with `SPREFIX=`. The original column stays beside the new one only with `OPREFIX`, which "specifies that the original variables should appear in the OUT= data set.". The 2 columns then need different names: "The value of OPREFIX= must be different from the value of SPREFIX=.". `OPREFIX` with no pre-fix of its own therefore needs `SPREFIX=`. Both methods should replace the columns by default, with a pre-fix option for the new columns (as `SPREFIX=` gives) and an option that keeps the original columns, with an optional pre-fix of their own (as `OPREFIX` gives). Because SAS requires a pre-fix for 1 of the 2 columns, the methods should raise an error when the original columns are kept with neither pre-fix given.
  - `rolling_window_analysis` adds a new column too, while its docstring shows `CONVERT column` with no new name. PROC EXPAND says "If a name for the output series is not given, the variable in the output data set has the same name as the input variable.". `rolling_window_analysis` should replace the column by default, with a new-name option (as `CONVERT column=newname` gives).

- `conditional_column_creation`:
  - `conditional_column_creation` does not follow SAS's rule that a missing numeric value is smaller than any number. In Spark, a comparison with null gives null, which `when` treats as false. A row with a null value therefore skips a condition that SAS would treat as true. The method should apply SAS's rule by default (as SAS does), with an option that keeps Spark's rule so that a caller whose code relies on Spark's rule can keep it.
  - The method also over-writes an existing column with null where no condition matches. In a PySpark 3.5.3 run, `conditional_column_creation("care_level", {"age >= 65": "X"})` gave X for 2 rows and null for the 8 rows that held A, B, C. In a DATA step with `SET`, the IF-THEN statement "executes a SAS statement for observations that are read from a SAS data set" only when its condition is met. The 8 rows that no condition matched therefore keep the value that `SET` read. The method should keep the old value where no condition matches.
  - A condition that is a number (such as `visits`) stopped with `DATATYPE_MISMATCH`, while in SAS, "A nonzero and nonmissing result causes the expression to be true". The method should test such a condition against 0 and null.

- `sql_query` follows Spark SQL's null rules, not PROC SQL's:
  - `sql_query` runs Spark SQL, where a comparison with null gives null. PROC SQL follows SAS's rule instead: "missing values in SAS always appear first in the collating sequence. Therefore, in Boolean and comparison operations, the following expressions resolve to true in a predicate: 3>null -3>null 0>null". In a PySpark 3.5.3 run, `WHERE age < 5` left out a row with a null age, which PROC SQL would return.
  - A PROC SQL join also pairs a missing key with a missing key, while a Spark join never matches 2 null keys. A post on SAS's blog shows the pairing: "Each missing value of ONE is combined with each missing value of TWO".

  `sql_query` should apply PROC SQL's 2 rules by default (a null is smaller than any number in a comparison, and `eqNullSafe` pairs null join-keys), with a test for each rule. The method's docstring should name any case that the method cannot translate so that a user knows which queries still differ from PROC SQL.

- `handle_missing`:
  - `handle_missing` fills an integer column with an integer. In a PySpark 3.5.3 run, the mean of 1 and 2 filled a null with 1, not 1.5. PROC STDIZE says "Missing values are replaced with the location measure", which is 1.5 here. The sample's `age` and `visits` columns are integers. The output column should therefore be a double.
  - With `method='median'`, the method uses Spark's `Imputer`, whose median comes from `approxQuantile` with a relative error of 0.001. PROC STDIZE computes percentiles with `PCTLMTD=ORD_STAT` when memory allows (with `PCTLDEF=5` by default), which gives the exact median. On the values 1, 2, 3, 10, the method filled a null with 2, where definition-5 gives 2.5. The method should use the exact median.
  - Its method-names do not match PROC STDIZE either. `method='mode'` ran, while the valid names are "MEAN, MEDIAN, SUM, EUCLEN, USTD, STD, RANGE, MIDRANGE, MAXABS, IQR, MAD, ABW, AHUBER, AWAVE, AGK, SPACING, L, and IN.". `method='midrange'` stopped with "parameter strategy given invalid value midrange". The method should take the SAS names that it can compute.
  - `Imputer` also casts the filled value back to the column's type. In a `decimal(10,2)` column holding 1.50, 2.25, null, the mean of 1.875 filled the null as 1.88 (on 3.5.3 and on 4.3.0). The method should fill with `coalesce` into a double column.
  - A long of 9007199254740993 that was not missing came out as 9007199254740992. SAS for UNIX (whose largest exact integer is 9,007,199,254,740,992) gives the same value because "Numeric values in SAS for UNIX are represented as IEEE double-precision floating-point numbers.". SAS on z/OS keeps the value (its largest exact integer is 72,057,594,037,927,936). The double column of the `coalesce` fix above matches SAS for UNIX. The case of the long value 9007199254740993 therefore needs no further fix.

- `calculate_means`:
  - `calculate_means` returns its groups in no set order. On 8 groups named a to h, 3 PySpark 3.5.3 runs with the notebook's settings each returned d, c, b, a, g, f, e, h. The `CLASS` statement of PROC MEANS defaults to `ORDER=UNFORMATTED`, which "yields the same order as PROC SORT.". The method should sort its output by the group-columns.
  - It also has no row-count per group. Unless `NONOBS` is given, PROC MEANS shows "the total number of observations for each unique combination of the values of the class variables.". In a PySpark 3.5.3 run, a group of 3 rows with 1 null value showed only a `v_count` of 2. The method should add each group's row-count.
  - Its count-column is `{col}_count` (where `{col}` is the analysis column's name), while `AUTONAME` gives "the combination of the analysis variable name and the statistic-keyword" (here `N`). The column should be `{col}_N`.

- `frequency_analysis`:
  - `frequency_analysis` names its columns `count` and `percentage`. PROC FREQ's `OUT=` data-set has "COUNT, which contains the table cell frequency" and "PERCENT, which contains the table cell percentage". To line up with a SAS output for `PROC COMPARE`, the method should re-name its `percentage` column to `percent`.
  - It also rounds the percentage to 2 decimals. The PROC FREQ page says nothing about rounding. To line up with a SAS output for `PROC COMPARE` (as in the column-name item above), the method should keep the full value.
  - Wherever the method shows a missing level (the null row that the PROC FREQ missing-value item above keeps for `OUT=` and the level that `MISSING` adds), that level should come 1st: "The ORDER= option does not apply to missing values, which are always ordered first.". In a PySpark 3.5.3 run on a, a, a, b, b, null, the null level came 3rd (sorted by its count).

- `cross_tabulation`:
  - `cross_tabulation` turns the row values into strings, as `crosstab` does. In a PySpark 3.5.3 run, the values 2 and 10 in the row column became the text "2" and "10". Sorted as text, 10 comes 1st. The method should keep the row column's type.
  - The rows also came back in no set order. PROC TABULATE's `ORDER=` defaults to `UNFORMATTED`, which "yields the same order as PROC SORT.". The method should sort the rows and columns by value.
  - Its `normalize=True` percentages are rounded to 2 decimals -- in a PySpark 3.5.3 run, a cell that held 1 of 3 rows gave 33.33. `cross_tabulation` should keep the full value so that it lines up with a SAS output for `PROC COMPARE` (as the `frequency_analysis` rounding item says).
  - In a PySpark 3.5.3 run with more than 10,000 distinct values in the column variable, the method stopped with "has more than 10000 distinct values", as `transpose_data` does. The method should count with `groupBy`, then pivot with the values given.
  - `crosstab` also fails on 3 kinds of value that SAS keeps apart (the `cross_tabulation` plain-bugs item adds a 4th -- a value that holds a back-tick). Each result below came from a PySpark 3.5.3 run:
    - The text value "null" and a missing value became 1 column `null` with a count of 2. A pivot with the values given still made 2 columns named `null`. In the row column, the same 2 values became 1 row `null` with a count of 2 (in PySpark 3.5.3 and 4.3.0 runs).
    - Binary values that do not decode as text (`b"\xff"` and `b"\xfe"`) became 1 column. A pivot with the values given gave NULL counts for them (4.3.0 gave the right counts under 2 columns that both print as "�"). In the row column, the same 2 values became 2 rows that both print as "�" (in PySpark 3.5.3 and 4.3.0 runs).
    - With `normalize=True`, the 2 column values m and M stopped the method with `AMBIGUOUS_REFERENCE`, though SAS treats them as 2 values. A pivot with the values given made the 2 columns m and M. The step that `normalize=True` adds then stopped with `AMBIGUOUS_REFERENCE` too.

    Because a pivot names its columns from the values, the `groupBy` route alone fixes none of the 3 cases on the column side. It fixes only the row side of the null case and of the binary case (in PySpark 3.5.3 and 4.3.0 runs, a `groupBy` on both columns kept the null row and the "null" row apart and kept the 2 binary values apart). The method should return its counts in long form (1 row per pair of values) or give each pivoted column a name that stays unique when case is ignored (Spark matches column names without regard to case because `spark.sql.caseSensitive` is `false` by default). For binary values, unique names are not enough on 3.5.3, where a pivot on the binary values gave NULL counts with or without new column-names. A pivot on a text form of each value (such as `hex`) gave the counts on both versions. Each of the 3 cases needs its own test (for the null case and the binary case, on both the row side and the column side).
  - The method also gives 0 for a combination with no row -- in a PySpark 3.5.3 run, rows p, p, p, q, q against x, y, y, x, x gave q a count of 0 under y. In PROC TABULATE's `PRINTMISS` example, cells with no data show missing values: "Because there are no data in these categories, the values for the statistics are all missing.". The method should return NULL there.

- `merge_datasets` and `transpose_data` return their rows in no set order. On 8 keys named a to h, a PySpark 3.5.3 run with the default settings returned the merged rows in sorted order, which Spark does not promise. With each side split by `repartition(4)`, the order changed with the data and the number of worker threads. Each result below came from a PySpark 3.5.3 run:
  - With 2 worker threads (the default `local[*]` on a 2-core machine), `merge_datasets` on keys a to h (each side with an integer column that holds 0 for key-a up to 7 for key-h) gave c, f, a, e, d, h, b, g. With the right side's column at 10 times those values, it gave c, f, a, g, b, e, d, h.
  - With 2 worker threads, `merge_datasets` (with its default inner join) on 2 other tables with the same keys (1 integer column on each side that holds 1 in every left row and 2 in every right row) gave c, h, d, f, a, g, b, e.
  - With 4 worker threads (`local[4]`), the same inner join on the 2 tables with 1 integer column on each side gave f, h, a, d, e, b, c, g.
  - `transpose_data` on the left table of the 1st run returned its BY groups as g, f, e, h, d, c, b, a.

  The data-sets in a `MERGE` "must be sorted in order of the values" of the BY variables. PROC TRANSPOSE with `BY` needs the observations "sorted by all the variables that you specify". `MERGE` and PROC TRANSPOSE both read their input in BY order. Each method should sort its output by the key or BY columns.

- `summarize_dataset`:
  - `summarize_dataset` shows 5 value-counts per column in no set order. The most frequent values can therefore be left out. On 8 string values ('1' to '8') with counts of 1 to 8, a PySpark 3.5.3 run showed the counts 3, 5, 6, 1, 4 in that order (leaving out 8 and 7). After `repartition(3)`, the method showed 7, 3, 8, 5, 6. The method should sort each column's counts from most to least frequent, as PROC FREQ's `ORDER=FREQ` does.
  - Each table of counts is headed `column_value` and `count`, with no column-name. A reader therefore cannot tell the tables apart. The method should print each column's name above its counts.
  - PROC CONTENTS also lists columns in alphabetical order: "By default, the CONTENTS statement lists the variables alphabetically.". The method lists them by position, as `VARNUM` does. The method's docstring should name `VARNUM` so that a reader knows that the method does not follow the default order of PROC CONTENTS.
  - The method prints a row-count but no column-count, while PROC CONTENTS shows both "Observations" and "Variables". The method should print the column-count too.
  - The method returns nothing, while PROC CONTENTS takes `OUT=` to "Specify the name for an output data set". The method should return a DataFrame with 1 row per column.
  - It stopped with `AMBIGUOUS_REFERENCE` on 2 columns that differ only in case and on the 2 columns of 1 name that `merge_datasets` leaves when both sides hold a non-key column of that name. The method should count each column by position.
  - It prints with `show`, which cuts every value longer than 20 characters to 17 plus "...". 2 values that share their 1st 17 characters therefore print identically. The method should print with `truncate=False`.

- Trailing blanks:
  - `conditional_column_creation`, `sql_query`, `merge_datasets` compare strings with their trailing blanks. SAS says "Since trailing blanks are ignored in a comparison, 'fox ' is equivalent to 'fox'.". In a PySpark 3.5.3 run, `'abc  '` did not match `'abc'` in a condition, a query, a join-key. By default (as SAS does), the 3 methods should drop trailing blanks before they compare, with an option that keeps them so that a caller whose data must keep `'abc  '` apart from `'abc'` can do so.
  - 7 more methods also treat `'abc  '` as a value apart from `'abc'`. Each result below came from a PySpark 3.5.3 run:
    - `frequency_analysis`, `calculate_means`, `cross_tabulation`, `transpose_data`, `rank_data` each made 3 groups from abc, abc with 2 trailing blanks, abd.
    - `format_values` labelled abc, not abc with 2 trailing blanks.
    - `sort_data` put `'abc '` after 2 `'abc'` rows that came later in the input, where SAS sees 3 equal values and keeps their order.

    The trailing-blank default of the item above should cover these 7 methods too.

- `frequency_analysis`, `calculate_means`, `cross_tabulation`, `transpose_data`, `rank_data` (for its BY groups) group by raw values, while SAS groups by formatted ones. The SAS pages give the rule for each procedure:
  - PROC FREQ: "PROC FREQ groups a variable’s values according to its formatted values.".
  - PROC MEANS: "If a numeric class variable is not assigned a format and you do not specify GROUPINTERNAL, then PROC MEANS uses the default format, BEST12., to format numeric values as character strings.".
  - PROC TABULATE: "the default format, BEST12., is used, unless the GROUPINTERNAL option is specified.".
  - PROC TRANSPOSE and PROC RANK (for their BY groups): SAS's page on the BY statement lists both among the Base procedures that support it and says "If the BY variable is numeric and has no user-applied format, then the BEST12. format is applied for the purpose of BY-group processing.".
  - For PROC FREQ, none of the SAS pages that were searched names the format of an unformatted numeric. The width of 12 comes from the `BESTw.` page ("Default:12").

  In a PySpark 3.5.3 run, 0.1+0.2 and 0.3 made 2 levels (2 BY groups in `rank_data`) in each method, as did 1234567890123 and 1234567890124. Under `BEST12.`, each pair prints as 1 value. That last claim needs a SAS run to confirm.

  The methods should group on the `BEST12.` form by default, with a `groupinternal` option so that code that uses `GROUPINTERNAL` keeps its groups of raw values.

- `conditional_column_creation` and `sql_query` read their text as Spark SQL, which lacks the 16 kinds of SAS syntax below or reads them differently:
  - SAS says "Comparison operators can be expressed as symbols or with their mnemonic equivalents" (such as `EQ`, `NE`, `GE`). SAS also takes `^=` for not equal. In a PySpark 3.5.3 run, the conditions `age eq 45`, `age ne 45`, `age ^= 45` each stopped with a `ParseException`, as did `WHERE age ge 65` in a query. The DATA step's table of comparison operators and PROC SQL's table of operators also list `¬=`, and `age ¬= 45` stopped both methods with `PARSE_SYNTAX_ERROR` on 3.5.3 and on 4.3.0. The DATA step's table also lists `~=` for not equal, and the DATA step takes the older forms `=>` and `=<`, of which SAS says "It is not supported in WHERE clauses or in PROC SQL.". On 3.5.3 and on 4.3.0, the conditions `age ~= 45`, `age => 62`, `age =< 42` each stopped with `PARSE_SYNTAX_ERROR`.
  - PROC SQL's `CALCULATED` "Refers to columns already calculated in the SELECT clause.". Spark stopped on it.
  - SAS's `PUT` function writes a value as text. Spark stopped on it.
  - PROC SQL also takes `IS MISSING`. SAS says "IS NULL and IS MISSING are predicates that test for a missing value.". Spark stopped on it with `PARSE_SYNTAX_ERROR`.
  - PROC SQL also takes `CONTAINS`, which "tests for values that contain a specified string". Spark stopped on it with `PARSE_SYNTAX_ERROR`.
  - SAS's own example "if a=b & c=d then x=1;" uses `&` for AND, while Spark reads `&` and `|` as bitwise operators. In a PySpark 3.5.3 run, `visits = 15 & age = 45` matched 0 rows, where SAS matches P001. `age = 45 | age = 62` matched 0 rows, where SAS matches 2. Neither condition gave an error. On 4.3.0, both conditions stopped with `DATATYPE_MISMATCH`. The DATA step's table of logical operators also lists `!` and `¦` for OR, and its table of the order of evaluation lists `¦¦` and `!!` for concatenation. On 3.5.3 and on 4.3.0, `age = 45 ! age = 62`, `age = 45 ¦ age = 62`, `(diagnosis !! 'x') = 'Cancerx'`, `(diagnosis ¦¦ 'x') = 'Cancerx'` each stopped both methods with `PARSE_SYNTAX_ERROR`.
  - SAS's table of logical operators lists `~` among its symbols for NOT and says "The symbol that you use for NOT depends on your operating environment.". `^(age > 50)` stopped with `PARSE_SYNTAX_ERROR`. `~(age > 50)` stopped with `DATATYPE_MISMATCH`. That table and PROC SQL's table of operators also list `¬`, and `¬(age > 50)` stopped both methods with `PARSE_SYNTAX_ERROR` on 3.5.3 and on 4.3.0.
  - In SAS, "the expression x<y<z is evaluated as (x<y) and (y<z).". In Spark, `18 <= age < 50` stopped with `DATATYPE_MISMATCH`.
  - The IN range in "if x in (0,9,1:5);" stopped with `PARSE_SYNTAX_ERROR`.
  - SAS's pre-fix comparison `=:` stopped with `PARSE_SYNTAX_ERROR`.
  - The missing literal `.` stopped both methods with `PARSE_SYNTAX_ERROR` (as in `temperature = .`).
  - PROC SQL's truncated comparisons `EQT`, `GTT`, `LTT`, `GET`, `LET`, `NET` each stopped both methods with `PARSE_SYNTAX_ERROR` on 3.5.3 and on 4.3.0 (as in `name eqt 'Ca'`). PROC SQL takes these operators where the DATA step takes `=:`: "Unlike the DATA step, PROC SQL does not support the colon operators (such as =:, >:, and <=:) for truncated string comparisons. Use the alphabetic operators (such as EQT, GTT, and LET).".
  - PROC SQL's sounds-like operator `=*` stopped both methods with `PARSE_SYNTAX_ERROR` on 3.5.3 and on 4.3.0 (as in `name =* 'Smith'`).
  - In a DATA step, `<>` and `><` are the MAX and MIN operators: "The MIN (><) operator returns the lower of the two values. The MAX (<>) operator returns the higher of the two values.". Spark reads `<>` as not equal. On 3.5.3 and on 4.3.0, `conditional_column_creation("k", {"age <> 45": "Y"})` marked 9 of the 10 rows without an error. SAS's IF marks all 10 because the larger of each age and 45 is neither 0 nor missing, and "In SAS, any numeric value other than 0 or missing is true". `(age >< 50) = age` stopped with `PARSE_SYNTAX_ERROR`.
  - Both the DATA step and PROC SQL take `**` for a power (the DATA step's table of arithmetic operators lists "** exponentiation", and PROC SQL's lists "** raises to a power"). On 3.5.3 and on 4.3.0, `age**2 > 4000` in a condition and `SELECT age**2` in a query each stopped with `PARSE_SYNTAX_ERROR`.
  - `MAX`, `MIN`, `SUM`, `MEAN` with 2 or more arguments work row by row in both the DATA step and PROC SQL. PROC SQL says "If you specify multiple arguments, then the arguments or columns that are listed are calculated for each row.". The DATA step's `MAX` function works within 1 row too (its SAS 9.4 example `x=max(8, 3);` gives 8). Spark's functions of those names take 1 column. On 3.5.3 and on 4.3.0, `max(age, visits) > 60` in a condition and `max(age, visits)`, `min(temperature, blood_pressure)`, `sum(age, visits)`, `mean(age, visits)` in a query each stopped with `WRONG_NUM_ARGS.WITHOUT_SUGGESTION`.

  Each method should translate only the kinds that its SAS counterpart takes:
  - `sql_query` should translate 12 of them (all but `=:`, `x<y<z`, the IN range, the MAX and MIN operators), without the `=>` and `=<` forms of the 1st kind. PROC SQL rejects `=:` (as the `EQT` item quotes) and the `=>` and `=<` forms (as the 1st item quotes). PROC SQL's table lists `< >` as "does not equal" and has no `><`. Spark's not-equal reading of `<>` therefore already matches PROC SQL. For not equal, NOT, OR, concatenation, that table lists only these symbols and key-words:

    - `¬=`, `^=`, `< >`, `ne` for not equal.
    - `¬`, `^`, `NOT` for NOT.
    - `|` and `OR` for OR.
    - `||` for concatenation.

    PROC SQL's table of operators does not list `~=`, `~`, `!`, `¦`, `!!`, `¦¦` and says only that "Symbols for operators might vary, depending on your operating environment.". None of the PROC SQL pages that were searched lists `x<y<z`, and PROC SQL's IN condition takes only constants or a query ("<NOT> IN (query-expression | constant <, ... constant>)"). `x<y<z`, the IN range, the `~=`, `~`, `!`, `¦`, `!!`, `¦¦` forms therefore wait on a test checked against a SAS run.
  - `conditional_column_creation` should translate 11 of them (all but `CALCULATED`, `IS MISSING`, `CONTAINS`, the `EQT` group, `=*`) because SAS takes those 5 kinds only in PROC SQL or a WHERE clause. SAS says "IS NULL and IS MISSING are used in the WHERE, ON, and HAVING expressions." and "The CONTAINS and LIKE operators are valid only in a WHERE clause.". The `EQT` group and `=*` are in PROC SQL's table of operators, while the DATA step's table of operators lists neither the `EQT` group nor `=*`.

  6 more parts of PROC SQL fail in `sql_query`. Each result below came from a PySpark 3.5.3 run:
  - SAS's `ROUND` reads its 2nd argument as a multiple -- it "Rounds the first argument to the nearest multiple of the second argument" (as in `round(1234.56789,.01)` giving 1234.57). Spark reads it as decimal places. The notebook's own query (`ROUND(AVG(length_of_stay), 2)`) gave 13.47, 12.93, 11.35, where SAS rounds to multiples of 2 (giving 14, 12, 12). The method should translate `ROUND`. Example-9 should use `0.01` so that its query still rounds to 2 decimals once `ROUND` follows SAS.
  - Re-merging failed -- `SELECT age, age - AVG(age) FROM temp_table` stopped with `MISSING_GROUP_BY`. PROC SQL re-merges. SAS says "The process of remerging involves two passes through the data.".
  - `GROUP BY` with no summary function stopped with `MISSING_AGGREGATION` when the SELECT list held another column (as in `SELECT * FROM temp_table GROUP BY diagnosis`). With only the group-column (as in `SELECT diagnosis FROM temp_table GROUP BY diagnosis`), the query returned 3 rows without an error. PROC SQL turns both queries into an `ORDER BY` with a warning in the log and returns all 10 rows.
  - The summary functions `N`, `NMISS`, `RANGE`, `CSS`, `USS`, `CV`, `FREQ`, `STDERR`, `T`, `PRT`, `SUMWGT`, `VAR` each stopped with `UNRESOLVED_ROUTINE`.
  - The SAS functions `UPCASE`, `LOWCASE`, `INDEX`, `SCAN`, `COMPRESS`, `CATX`, `MISSING`, `STRIP`, `MDY`, `TODAY` each stopped with `UNRESOLVED_ROUTINE` too.
  - `TRIM` and `LENGTH` ran but gave results that differ from SAS's. Spark's `TRIM` cut both ends, while SAS's `TRIM` "Removes trailing blanks". `LENGTH('ab  ')` gave 4, while SAS's `LENGTH` excludes trailing blanks -- it gives 2.

  The method should translate what it can. Its docstring should name what Spark SQL does not take so that a user knows which PROC SQL code still needs a re-write.

- `format_values`:
  - `format_values` cannot apply a numeric format. With string keys on an integer column, it changed nothing without a warning. With integer keys, Spark stopped with `MIXED_TYPE_REPLACEMENT`. Both results came from a PySpark 3.5.3 run. PROC FORMAT's `VALUE` statement makes numeric formats too since only "a character format" takes the `$`. The method should write the labels to a new string column, as `PUT` does. A new parameter should name that column (`formatted_col` in the method's docstring). The parameter should have a default name so that existing calls (such as example-6) still run. The method should raise the name-clash error of the plain-bugs output-name item when that name is taken.
  - The `VALUE` statement also takes ranges (such as `0-<100`) with the key-words `LOW`, `HIGH`, `OTHER`. SAS says "OTHER matches all values that do not match any other value or range.". The method takes only exact values (through `replace`). The method should take ranges and an `OTHER` label.
  - It cannot label a missing value either. A key of `None` stopped it with `MIXED_TYPE_REPLACEMENT`, while SAS's `VALUE` statement takes `.` as a key (as in `value xscore .='score unavailable';`). The method should take a missing-value key.
  - SAS also sets a format's width: "The default length of a format is the length of the longest formatted value.". The method kept an unmatched 17-character code at full length, where the PROC FORMAT `VALUE` statement page implies that SAS cuts it to the longest label. That cut needs a SAS run to confirm. Once it is confirmed, the method should cut to the same width.

- `correlation_analysis`:
  - `correlation_analysis` returns 1 cell that holds the matrix, with no variable-name. A reader therefore cannot tell which coefficient belongs to which pair of variables. The method should return a table with 1 row and 1 column per variable.
  - PROC CORR also shows a p-value for each coefficient and simple statistics for each variable (its `NOPROB` and `NOSIMPLE` options turn them off). The method should therefore also return the p-values and the simple statistics.
  - PROC CORR's table shows "the number of nonmissing observations for each pair of variables" because pairwise deletion gives each pair its own count. The method's table should hold that count too.
  - PROC CORR's `KENDALL` option "requests a table of Kendall’s tau-b coefficients". In a PySpark 3.5.3 run, `method='kendall'` stopped with "Unrecognized method name. Supported correlations: pearson, spearman". The method should compute Kendall's tau-b itself.
  - The method-name is case-sensitive -- `method='PEARSON'` (the spelling of its own docstring) stopped with "Unrecognized method name". SAS says "You can use uppercase, lowercase, or mixed case in the code that you write.". The method should lower-case the name that it is given.
  - On a constant column, the method gave NaN for that column's coefficient with each other column (its diagonal entry stayed 1.0). On 1 row, it stopped with "computeCovariance called on matrix with only 1 rows". None of the SAS pages that were searched gives PROC CORR's result for either case. The method should return NULL in both cases (for the constant column because SAS has no NaN and for 1 row so that the call does not stop).

- `standardize_data`, `rank_data`, `handle_missing`, `rolling_window_analysis` take case-sensitive method-names too (as the case-sensitivity item under `correlation_analysis` says of that method). `rolling_window_analysis("age", 2, "MEAN")` stopped with "Unsupported aggregation function: MEAN". Each should lower-case the name that it is given.

- An empty column list does not follow SAS's rule for an omitted `VAR` or `TABLES` statement. Each result below came from PySpark 3.5.3 and 4.3.0 runs:
  - `calculate_means` stopped with "exprs should not be empty".
  - `handle_missing` stopped with "inputCols cannot be empty".
  - `correlation_analysis` stopped with a `Py4JJavaError` from `Correlation.corr`.
  - `standardize_data` returned the data unchanged without a warning.
  - `frequency_analysis` returned 1 row that holds only a count and a percentage of 100.0.

  SAS gives each procedure a default instead:
  - PROC MEANS: "If you omit the VAR statement, then PROC MEANS analyzes all numeric variables that are not listed in the other statements.".
  - PROC STDIZE: "If you omit the VAR statement, all numeric variables not listed in the BY, FREQ, and WEIGHT statements are used.".
  - PROC CORR: "If you omit the VAR statement, all numeric variables not listed in other statements are used.".
  - PROC FREQ: "If you omit the TABLES statement, PROC FREQ generates one-way frequency tables for all data set variables that are not listed in the other statements.".

  Each method should read an empty list as SAS's default or raise an error that names the problem so that no call stops on an internal error or returns the data unchanged.

- 9 methods handle a bad argument without a clear error. Each result below came from a PySpark 3.5.3 run:
  - `standardize_data` returned the data unchanged for `method='std'` and `method='range'` (the names that its own docstring shows as `METHOD=STD` and `METHOD=RANGE`). The method should take `STD` as an alias of `zscore` and `RANGE` as an alias of `minmax`. It should raise an error on any other value.
  - `rolling_window_analysis` filled the new column with nulls for a window size of 0 or -2. It should raise an error for a size below 1.
  - `rank_data` stopped with a bare `KeyError: 'mean'` for an unknown method. The method should raise an error that names the bad value and lists the methods that it takes.
  - `rolling_window_analysis` names the bad value but does not list its methods. Its error should list them too.
  - A single name where a list is expected stopped 7 methods:
    - `sort_data("age")` read the string letter by letter and stopped on a column `a`.
    - `rank_data(partition_cols="gender")` also read the string letter by letter (stopping on a column `g`).
    - `calculate_means`, `frequency_analysis`, `standardize_data` also read the string letter by letter (stopping on the columns `d`, `g`, `a`).
    - `handle_missing("temperature")` and `correlation_analysis("age")` each stopped with "Could not convert" plus the name, then "to list of strings".

    Each should take a single name, as PySpark's own `sort` and `groupBy` do.
  - `sort_data(ascending=0)` stopped with "'int' object is not iterable". `sort_data` should take a 0 or 1, as PySpark's own `sort` does for `ascending`.
  - `merge_datasets` with an empty `on` list stopped with a bare `IndexError`. SAS says "To perform a one-to-one merge, use the MERGE statement without a BY statement.". The method should either raise an error that names the problem or merge by position.
  - `rolling_window_analysis` stopped with a `Py4JError` for a window size of 2.0 and with `INVALID_BOUNDARY` for 3,000,000,000. It should take an integer of 1 or more and raise a clear error when the window's start falls outside the range that Spark accepts ("[-2147483648, 2147483647]" in its `INVALID_BOUNDARY` message).

- The hint of `conditional_column_creation` says `Dict[str, str]`, while numbers, dates, booleans, None work as values. It should be `dict[str, Any]` (the built-in form that the `ruff` item below asks for), with `Any` added to the notebook's `from typing import` line. Without that import, the class definition stops with `NameError` because Python evaluates the hint when it defines the method.

- `standardize_data`, `handle_missing`, `rolling_window_analysis`, `univariate_analysis`, `correlation_analysis`, `frequency_analysis` have no BY option. In a PySpark 3.5.3 run, a 2-row moving average from `rolling_window_analysis` averaged the 1st row of group-b with the last row of group-a. In PySpark 3.5.3 and 4.3.0 runs on 2 groups, `correlation_analysis` gave 1 pooled coefficient of 0.3698, where the coefficient is 1.0 within group-a and -1.0 within group-b. `frequency_analysis` gave each percentage out of all rows, where a BY group gets its own percentages. SAS says "A BY statement can be used with PROC EXPAND to obtain separate analyses on observations in groups defined by the BY variables.". PROC STDIZE, PROC UNIVARIATE, PROC CORR, PROC FREQ take one too (for PROC CORR, "You can specify a BY statement in PROC CORR to obtain separate analyses of observations in groups that are defined by the BY variables."). Each method should take BY columns so that each group gets its own statistics.

- `cross_tabulation` names its row column `diagnosis_gender` (the 2 names joined by an under-score). PROC TABULATE's `OUT=` data-set keeps each CLASS variable under its own name (as "variables that are listed in the CLASS statement"). The method should name the column `diagnosis`.

- 12 plain bugs (each from a PySpark 3.5.3 run):
  - `univariate_analysis` stopped on an all-null column and on an empty DataFrame with "9 columns passed, passed data had 0 columns" because `approxQuantile` returns an empty list there. The method should return NULL for each statistic there (SAS has no NaN) so that 1 all-null column or an empty DataFrame does not stop the call.
  - `handle_missing` stopped on an all-null column and on an empty DataFrame with "surrogate cannot be computed.". It should leave that column as it is (with a warning) so that 1 all-null column or an empty DataFrame does not stop the whole call.
  - `correlation_analysis` stopped on an all-null column and on an empty DataFrame with "Cannot determine the number of cols". It should return NULL coefficients (as the constant-column item under `correlation_analysis` asks) so that 1 all-null column or an empty DataFrame does not stop the whole call.
  - `conditional_column_creation` with an empty dict of conditions made a column of type `void`, which Parquet refused on 3.5.3 (4.3.0 wrote it). The method should raise an error for an empty dict.
  - `cross_tabulation` with dots and back-ticks:
    - With `normalize=True`, `cross_tabulation` stopped with `UNRESOLVED_COLUMN.WITH_SUGGESTION` when a column value held a dot (such as 1.5) because `F.col` reads the dot as a field-name. The method should quote with back-ticks each column-name that it builds from a value.
    - `crosstab` also dropped the back-tick from a value that held one (``a`b`` became the column `ab`). The long-form route of the `crosstab` item under `cross_tabulation` above keeps each value as data, which fixes the back-tick fault too.
  - The dot fault of the `cross_tabulation` item above hits column-names too. With columns named a.b and g.h, 13 of the 18 methods stopped with `UNRESOLVED_COLUMN.WITH_SUGGESTION`. On 4.3.0, `handle_missing` and `correlation_analysis` stopped with `FIELD_NOT_FOUND` instead. The other 5 methods ran:
    - `merge_datasets`.
    - `conditional_column_creation` with back-ticked names in its conditions.
    - `sql_query` with back-ticked names.
    - `split_dataset`.
    - `time_series_forecasting`.

    CSV and Parquet both allow such names. Every method should quote each name with back-ticks wherever Spark reads the name as a column reference because Spark reads a dot in an unquoted name as a field access. Each back-tick inside a name should be doubled because Spark stops on a name that holds a single back-tick (a column named ``a`b`` resolves only as ``` `a``b` ```). `merge_datasets` should keep passing the names to `join(on=...)` as they are because a back-ticked name stopped the join with `UNRESOLVED_USING_COLUMN_FOR_JOIN` on both versions. Output-names should also stay unquoted because `crosstab` kept the back-ticks in the name of its 1st column (`` `a.b`_`g.h` ``).

    On 3.5.3, `Imputer` and `VectorAssembler` refused a back-ticked name ("`` `a.b` does not exist. Available: a.b, g.h ``"), while 4.3.0 took it. `handle_missing` and `correlation_analysis` should therefore re-name each column to a name with no dot or back-tick before those steps and restore the name after.
  - `standardize_data(method='minmax')` stopped on an all-null column and on an empty DataFrame with "unsupported operand type(s) for -: 'NoneType' and 'NoneType'". It should check for a NULL minimum or maximum and return the column as it is (as for a constant column) so that the call does not stop with a Python error.
  - Long and integer arithmetic wrapped around:
    - `rolling_window_analysis` with `sum` and a window size of 2 on 9223372036854775807 and 1 gave -9223372036854775808 on 3.5.3 and stopped with `ARITHMETIC_OVERFLOW` on 4.3.0. SAS keeps every number in floating point -- SAS for UNIX uses "IEEE double-precision floating-point numbers", and SAS on z/OS stores "all numeric values in 8-byte floating-point (real binary) representation.". A SAS sum therefore never wraps. The method should cast to double before it adds.
    - Integer arithmetic wraps in `sql_query` and `conditional_column_creation` too. In `sql_query`, `age * 100000000` gave 205032704 for an age of 45, and `SUM` on 9223372036854775807 and 1 gave -9223372036854775808. In `conditional_column_creation`, the condition `age * 100000000 = 205032704` held for that row. On 4.3.0, each of these 3 calls stopped with `ARITHMETIC_OVERFLOW`. Because SAS keeps every number in floating point (as the `rolling_window_analysis` sum item above says), both methods should cast integer columns to double before arithmetic.
  - The notebook sets `spark.sql.legacy.timeParserPolicy` to `LEGACY` for the whole session. The 3 time-parser policies (`LEGACY`, `EXCEPTION`, `CORRECTED`) gave these results:
    - With `LEGACY`, `to_date(..., 'yyyy-MM-dd')` read '2024-01-05 junk' as January 5, 2024, without a warning.
    - With `LEGACY`, `to_date` also read '24-01-05' as the year-24.
    - Without the notebook's `timeParserPolicy` line, PySpark 3.5.3 uses its default `EXCEPTION` policy, where both reads stopped with `INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER`.
    - On 4.3.0 (whose default is `CORRECTED`), ANSI mode stopped both reads with `CANNOT_PARSE_TIMESTAMP`.
    - Under `CORRECTED` with ANSI off, both reads gave NULL.

    No method parses dates in its own code, but `sql_query` and `conditional_column_creation` run the caller's `to_date` under the session's policy. In PySpark 3.5.3 and 4.3.0 runs under the notebook's `LEGACY` line, `sql_query` read '2024-01-05 junk' as January 5, 2024. The `timeParserPolicy` line should therefore go so that a caller's query or condition parses dates under Spark's default policy.
  - `rank_data` with no BY column and `rolling_window_analysis` move all input rows to 1 partition, and `cross_tabulation(normalize=True)` moves all rows of its crosstab (1 per row value) to 1 partition. In PySpark 3.5.3 and 4.3.0 runs, Spark logged "No Partition Defined for Window operation! Moving all data to a single partition" 14 times over the worked examples. Each method needs 1 change:
    - The `cross_tabulation` totals should come from 1 aggregate so that `normalize=True` no longer moves every crosstab row to 1 partition.
    - The docstring of `rolling_window_analysis` should say that the method runs on 1 partition when no BY column is given so that a user with a large data-set knows the cost before a run.
    - The docstring of `rank_data` should say that the method runs on 1 partition when no BY column is given so that a user with a large data-set knows the cost before a run.
  - When `frequency_analysis` counted a column named `percentage`, it over-wrote that column with its own output. Counting a column named `count` stopped it with `AMBIGUOUS_REFERENCE`. 8 more methods also fail when an output-name is already taken:
    - `rank_data`, `standardize_data`, `rolling_window_analysis`, `handle_missing` over-wrote an existing output column without a warning.
    - `calculate_means`, `cross_tabulation`, `transpose_data` made 2 columns of 1 name.
    - `cross_tabulation(normalize=True)` on the column values x and x_pct replaced the x_pct counts with percentages without a warning and added a column named `x_pct_pct`.
    - `correlation_analysis` stopped when the data held a column named `correlation_features`.

    Each should raise an error when an output-name is taken. For `standardize_data`, `handle_missing`, `rolling_window_analysis` (which replace their input column by default once the fix of the new-columns item is in), the error applies only when a pre-fix or new name is given.

    Each internal column needs a unique name so that it cannot clash with a column of the input.
  - `sql_query` replaced a view that the caller had named `temp_table`. The method should register its view under a name of its own (taken as a parameter). Its docstring and example-9 should use that name in `FROM`.

- 8 methods run more Spark jobs than they need (counted with Spark's status tracker in a PySpark 3.5.3 run on the sample data, with the calls that the tests make and, for `summarize_dataset`, the call of worked example-15):
  - `frequency_analysis` runs 2 for its row-count before any action.
  - `standardize_data` runs 8 for 2 columns, where 1 `select` of every statistic runs 2.
  - `univariate_analysis` runs 3 (1 for `approxQuantile` and 2 for the `toPandas` step of its pandas round-trip).
  - `summarize_dataset` runs 26 on the 11-column sample.
  - `handle_missing` runs 4 for 2 columns (the `Imputer` fit and transform), where 1 `select` of the 2 means runs 2.
  - `correlation_analysis` runs 4 for 3 columns, where 1 `select` of the 3 `corr` pairs runs 2.
  - `cross_tabulation` runs 4 in `crosstab`, where a `distinct` of the column values and a pivot with those values run 2.
  - `transpose_data` runs 4 in its pivot without values, where a `distinct` of the ID values and a pivot with those values run 2.

  Each method should run no more Spark jobs than its item names:
  - `frequency_analysis`, `standardize_data`, `univariate_analysis`, `summarize_dataset`, `handle_missing`, `correlation_analysis` should take their statistics in 1 pass.
  - `cross_tabulation` and `transpose_data` should take a `distinct` of the values, then pivot with those values.

- `time_series_forecasting` is a place-holder that does nothing. Remove it with worked example-17 and with these README parts so that the README neither names a method that no longer exists nor counts an example that is gone:
  - The `PROC ARIMA` table row.
  - The aside in the 1st paragraph.
  - The "(except `PROC ARIMA`)" of the 2nd paragraph.
  - The count in "the 17 worked examples", which becomes 16.

## Packaging

- The notebook's 1st cell starts with a comment line, then `%%capture`. A cell magic must be the 1st line of a cell. In IPython 9.18.1, the cell stopped with "``UsageError: Line magic function `%%capture` not found.``". The install, the imports, the Spark session therefore never ran. `%%capture` should move to line-1.

- 3 worked examples change with the fixes. Each should be re-written with its fix:
  - 9, for the `temp_table` view and `ROUND`.
  - 13, for the order-by column.
  - 14, which must drop its inner quotes (`"Senior"` in place of `"'Senior'"`) if the quote-character fix of the 1st `conditional_column_creation` item takes the plain-value route.

  Example-17 goes with `time_series_forecasting`.

- Move the class out of the notebook into an installable package (`pyproject.toml` and a `src/` lay-out) with the notebook kept as a demo so that the class can be installed and imported without running the notebook.

- Move the `unittest` suite out of the notebook into `pytest` files with a shared SparkSession fixture so that CI can collect the tests and every test gets its session from 1 fixture once the package no longer creates a global `spark`.

  6 of the 13 tests (7 if `cross_tabulation` takes the long-form route) fail once the fixes above are applied. Each of them changes with its fix:
  - `test_frequency_analysis` checks for a `percentage` column.
  - `test_standardize_data` and `test_handle_missing` check for the `_standardized` and `_imputed` columns.
  - `test_correlation_analysis` reads the matrix from the 1st cell of the 1st row.
  - `test_format_values` checks `care_level` for the labels, while the new-column fix of `format_values` writes them to a new column and leaves the codes in `care_level`.
  - `test_sql_query` queries `temp_table`, which `sql_query` will no longer register.
  - `test_cross_tabulation` checks for an `M_pct` or `F_pct` column, which the long-form route does not make.

  None of the 13 tests compares a value with what SAS gives. They check only these properties:
  - Column-names.
  - Row-counts.
  - A column-count (in `test_merge_datasets`).
  - Sort-order.
  - A 0-to-1 range.
  - Symmetry.
  - The absence of nulls.
  - That every `care_level` value is a format label.

  The test that comes with each fix should check a value worked out by hand from the SAS rule that the fix cites.

- Pin and test the supported PySpark and Python versions:
  - The suite passes on PySpark 3.5.3, where Python 3.12 or later needs `setuptools` because `pyspark.ml` and PySpark's pandas conversion import `distutils`. On PySpark 4.3.0 with Python 3.13 and pandas 3.1.1, the 13 tests pass and the worked examples run without `setuptools`. The pins should therefore add `setuptools` for PySpark 3.5.3 on Python 3.12 or later.
  - On Java 25, PySpark 3.5.3 stopped when it created its SparkContext ("getSubject is not supported" from Hadoop's `UserGroupInformation`), while 4.3.0 passed the 13 tests. The Spark 3.5.3 docs say "Spark runs on Java 8/11/17, Scala 2.12/2.13, Python 3.8+, and R 3.5+.". The 13 tests passed on Java 21 all the same. The pins and CI should name the Java versions (17 or 21 for PySpark 3.5.3) so that a newer JDK does not stop the suite before its 1st test.
  - PySpark 4.3.0 needs Python 3.11 or later, while 3.5.3 takes Python 3.8 or later (their PyPI metadata gives `>=3.11` and `>=3.8`). The PyPI classifiers of 3.5.3 list only Python 3.8 to 3.11, while the 13 tests passed on Python 3.13 all the same. The pins should therefore name the Python versions that CI tests for 3.5.3. The Spark 4.3.0 docs say "Spark runs on Java 17/21/25, Scala 2.13, Python 3.11+, and R 4.0+ (Deprecated).". The pins should name Python 3.11 as the minimum for 4.3.0 so that a reader on an older Python learns of that minimum before the install fails.
  - PySpark 4.3.0 warns that it "does not yet fully support pandas >= 3.0.0.". pandas 3 warns on PySpark 3.5.3 too -- in a PySpark 3.5.3 run of `test_univariate_analysis` (unittest shows deprecation warnings), pandas 3.1.1 raised 6 `Pandas4Warning` warnings ("The copy keyword is deprecated") that pandas 2.3.3 did not raise. In a PySpark 4.3.0 run of the 13 tests as cell-5 runs them, pandas 3.1.1 raised the same 6 warnings. On PySpark 3.5.3, the `is_datetime64tz_dtype` deprecation appeared under both pandas versions (as a `Pandas4Warning` under pandas 3.1.1 and as a `DeprecationWarning` under pandas 2.3.3). The 4.3.0 run did not raise it. A plain call of `univariate_analysis` on PySpark 3.5.3 printed none of these warnings under either pandas version. The pins should therefore keep pandas below 3 on both versions until PySpark fully supports pandas 3.
  - Spark 4.0 turns ANSI mode on by default. The division by 0 in `standardize_data` therefore stopped with `DIVIDE_BY_ZERO` on 4.3.0 instead of returning `NULL`. ANSI mode changes more than that division. On 4.3.0, these calls stopped, where 3.5.3 ran them without an error:
    - On the sample data, `conditional_column_creation` with the conditions `age < 50` (giving 1) and `age >= 50` (giving "old") stopped with `CAST_INVALID_INPUT`. On 3.5.3, it returned the text '1' for the ages under 50. When no row reached the "old" branch (as with `age > 500`), 4.3.0 ran without an error.
    - On the sample data (whose `province` column holds text such as ON), the condition `province = 1` stopped with `CAST_INVALID_INPUT` in `conditional_column_creation` and in a `sql_query` WHERE.
    - A `merge_datasets` (inner or outer) of an integer key with the string key 'z' stopped with `CAST_INVALID_INPUT` too.
    - `calculate_means`, `standardize_data`, `rolling_window_analysis` on a string column holding x1 and x2 stopped with `CAST_INVALID_INPUT`. On 3.5.3, they returned NULL for each computed value (`calculate_means` still gave x1 as the minimum, x2 as the maximum, a count of 2).
    - `standardize_data(method='minmax')` on 1 row stopped with `DIVIDE_BY_ZERO`.

    The class should cast each column and check each divisor itself so that a result does not depend on the ANSI setting.

    CI should run the tests on both versions with ANSI on so that the tests fail on both versions for each cast or division that ANSI mode stops.
  - 4.3.0 also returns binary values as `bytes`, where 3.5.3 gives `bytearray`. A test that compares binary values should therefore compare `bytes(...)`.
  - Both versions log each failed Spark task at ERROR level, and ANSI mode makes 4.3.0 fail tasks that 3.5.3 completes (the ANSI cases above). CI should therefore not treat ERROR lines in the log as failures.

- Add a GitHub Actions job that runs the tests on each push so that a change that breaks a test fails on the push that made it.

- Add `ruff` and `mypy` checks to the GitHub Actions job so that a lint or type fault fails the push that made it. On today's code, these tools report faults:
  - With ruff's F rules on, ruff 0.17.4 flags `from pyspark.sql.types import *` (F403) with 36 uses of the 5 names that it supplies (F405), plus 2 unused imports (`Tuple` and `pytest`). The star-import should name the 5 types that the code uses, and the 2 unused imports should go.
  - Ruff's default rule-set reports 22 findings (the 2 unused imports among them). 18 of them flag older type-hint forms (15 findings on 13 hints under UP006, UP045, UP007 plus 3 names of the `typing` import under UP035). 2 of the 13 hints carry 2 findings each (`Union[bool, List[bool]]` and `Optional[List[str]]`). The last 2 of the 22 findings are PIE790 (a `pass` that is not needed) and I001 (import-order). The 13 hints should move to the newer forms that these rules name. The `typing` import should then shrink to `from typing import Any` (the 1 name that the `conditional_column_creation` hint item still needs) because `List`, `Dict`, `Optional`, `Union`, `Tuple` are then unused (ruff 0.17.4 flagged all 5 under F401 once the hints had moved). PIE790 and I001 should be fixed too so that the job passes. With all the fixes of this item applied (PIE790 and I001 included), ruff 0.17.4 reported "All checks passed!" with its default rules and with its F rules.
  - mypy 2.4.1 (run against PySpark 3.5.3 with `pandas-stubs` and `pytest` installed) reports 2 errors:
    - The `split_dataset` hint above.
    - `format_values` passing a `dict[str, str]` where `replace` expects `dict[bool | float | int | str, bool | float | int | str | None]` (`dict[str, str]` does not match it because a `dict` type is invariant). The new-column and range items under `format_values` above remove that `replace` call since ranges and a new column need a mapping expression.

- The notebook's install-line pulls in `scipy` and `scikit-learn`, which no cell imports. The package's dependency list should leave them out.

- Tag a v0.1.0 release once the package and CI are in place so that users can pin the 1st installable version.

## New SAS equivalents

- Add DATA-step BY-group processing with `FIRST.` and `LAST.` flags since no method in the README's table covers it. With 2 or more BY variables, "SAS sets the value of FIRST.variable to 1 for the variable whose value changed, as well as for all of the variables that follow in the BY statement.". In a PySpark 3.5.3 run on the rows (a, x), (a, y), (b, y) with the BY variables g and h, a `FIRST.h` flag that tests only h gave 0 for the 3rd row, where SAS gives 1 because g changes there. The new method should follow SAS's rule.

- Add `LAG` through `lag` over an ordered window since no method in the README's table covers it. Spark's `lag` matches `LAG` only when `LAG` runs on every row: "An occurrence of the LAGn function that is executed conditionally will store and return values only from the observations for which the condition is satisfied.". The new method's docstring should therefore say that the method matches only a `LAG` that runs on every row.

- Add 2 `RETAIN` patterns (a running total and a value carried forward) through cumulative windows since no method in the README's table covers them. A running total needs SAS's 2 forms, each with its own option:
  - The sum statement treats a missing value as 0 and starts at 0. A cumulative `sum` matches it only inside `coalesce(..., 0)` -- on a double column holding null, null, 5.0, a cumulative `sum` gave NULL, NULL, 5.0 in a PySpark 3.5.3 run.
  - With `RETAIN tot 0; tot = tot + x;` (where `tot` is the running total and `x` is the input column), the total turns missing from the 1st missing value on. A cumulative `sum` skips a null instead (on a double column holding 1.0, null, 2.0, it gave 1.0, 1.0, 3.0 in a PySpark 3.5.3 run). This form therefore needs a running flag that turns the total NULL from the 1st null on.

- Add DATA-step `SET` with 2 or more input data-sets since `merge_datasets` covers only `MERGE`. `SET` matches columns by name: "The values of variables found in one data set but not in another are set to missing.". Spark's `union` matches by position. In PySpark 3.5.3 and 4.3.0 runs, when the 2 inputs listed 2 string columns in different orders, `union` swapped their values without an error. On 4.3.0, `union` stopped with `CAST_INVALID_INPUT` when it paired an integer column with a string column that held the text `s`. When the string column held only digits (7 and 8) instead of `s`, `union` paired the 2 columns without an error on both versions (4.3.0 cast the digits to integers, while 3.5.3 cast the integers to text). The `SET` method should therefore use `unionByName(allowMissingColumns=True)`. `unionByName` still pairs a numeric column with a string column of the same name. In a PySpark 3.5.3 run, it cast the integer 1 to the text '1'. In a 4.3.0 run, it cast the text 7 and 8 to integers and stopped with `CAST_INVALID_INPUT` on `s`. SAS stops instead: "ERROR: Variable EmployeeID has been defined as both character and numeric.". The `SET` method should therefore also raise an error when a column-name has a numeric type in 1 input and a string type in another so that no value is cast without a warning.

- Add `PROC APPEND` since `merge_datasets` covers only `MERGE`. `PROC APPEND` needs `FORCE` for a column that the base data-set lacks. The `PROC APPEND` method should therefore raise an error unless asked to drop that column.

- Add `PROC SQL` queries across 2 or more DataFrames since `sql_query` registers only 1 view (for its own DataFrame).

- Add `PROC COMPARE` to compare 2 DataFrames by key and report the rows and values that differ so that converted code can be checked against a SAS output exported to CSV. The method must follow 3 PROC COMPARE rules:
  - PROC COMPARE matches rows by position unless an `ID` statement names keys: "In a simple comparison, PROC COMPARE uses the observation number to determine which observations to compare.".
  - PROC COMPARE's rule that "a missing value is judged equal only to a missing value of the same type" needs `eqNullSafe`.
  - The method must take PROC COMPARE's 4 `METHOD=` values (`EXACT`, `ABSOLUTE`, `RELATIVE`, `PERCENT`) and `CRITERION=` so that a check gives the result that PROC COMPARE gives with the same options. SAS says "Unless you use the CRITERION= option, the default method is EXACT.". With `CRITERION=` alone, the default becomes `RELATIVE`.

- Add the SAS date functions `INTNX` (with the `BEGINNING`, `MIDDLE`, `END`, `SAME` values of its alignment argument) and `INTCK` (with its `DISCRETE` and `CONTINUOUS` methods) through Spark date functions since no method in the README's table covers them. Each rule below needs a test:
  - `INTNX` aligns to `BEGINNING` by default, while Spark's `add_months` keeps the day, as `SAME` does.
  - `INTCK` counts by `DISCRETE` by default: "For the DISCRETE method, the distance in months between January 31, 2000, and February 1, 2000, is one month.". Spark's `months_between` gave 0.03225806.
  - SAS weeks start on Sunday: "In general, weekly intervals begin on Sunday, and SAS counts a week whenever it passes a Sunday.". Spark's week truncation starts on Monday.

- Add DATA-step `ARRAY` with a `DO` loop that runs over a list of columns since no method in the README's table covers it.

- Add `PROC IMPORT` for CSV files (with Parquet as a 2nd format on the Spark side) since no method in the README's table reads files. PROC IMPORT guesses types from 20 rows by default (`GUESSINGROWS`) and turns a space in a column-name into an under-score. "Occupancy Code" becomes Occupancy_Code. Spark's `inferSchema` reads every row and keeps the name. The `PROC IMPORT` method needs options that follow SAS.

- Add `PROC EXPORT` for CSV files (with Parquet as a 2nd format) since no method in the README's table writes files. PROC EXPORT writes 1 file, while Spark's `write.csv` wrote a folder of part files. The `PROC EXPORT` method needs an option that follows SAS.

## Documentation and sample data

- Replace the invented health-themed sample data with an invented retail-sales data-set so that the examples show a general-purpose tool. The 13 tests and 14 of the worked examples (all but example-15 and example-16, with example-17 removed by the `time_series_forecasting` item) read the current data's columns by name (among them `diagnosis`, `care_level`, `length_of_stay`, `temperature`, `blood_pressure`). Each of those tests and examples changes with the new data-set.

  The new data-set's dates should be a date column, not text as `admission_date` is today. The dates and time-stamps item and the `INTNX` and `INTCK` item above need real dates so that their tests can run on the sample data.

- Add a SAS-to-PySpark reference page with 1 short before-and-after example per procedure so that a reader can see each mapping without opening the notebook.
