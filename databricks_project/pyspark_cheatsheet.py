from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =============================================================================
# IMPORTS
# =============================================================================

# import PySpark SQL functions as F for convenient access
# import Window for window-based analytics like row_number, lag, running totals

# =============================================================================
# COLUMN SELECTION AND CREATION
# =============================================================================

# select specific columns from a DataFrame
df.select("id", "name")

# select columns using expressions; alias renames the output column
df.select(F.col("id"), F.upper("name").alias("NAME"))

# add a new column with the current Spark timestamp
df = df.withColumn("created_at", F.current_timestamp())

# add a constant/literal value to every row
df = df.withColumn("constant", F.lit(1))

# create a derived column using arithmetic on existing columns
df = df.withColumn("total", F.col("price") * F.col("qty"))

# rename a column
df = df.withColumnRenamed("old_name", "new_name")

# remove a column
df = df.drop("unused_col")

# =============================================================================
# FILTERING ROWS
# =============================================================================

# keep only rows where age is greater than 18
df.filter(F.col("age") > 18)

# filter with multiple conditions; & means AND
df.where((F.col("age") > 18) & (F.col("country") == "US"))

# keep rows where name is not null
df.filter(F.col("name").isNotNull())

# keep rows where score is null
df.filter(F.col("score").isNull())

# =============================================================================
# STRING FUNCTIONS
# =============================================================================

# convert string to lowercase
F.lower("name")

# convert string to uppercase
F.upper("name")

# capitalize first letter of each word
F.initcap("name")

# remove spaces from both ends of a string
F.trim("name")

# remove spaces from left side
F.ltrim("name")

# remove spaces from right side
F.rtrim("name")

# get string length
F.length("name")

# extract substring starting at position 1 with length 3
F.substring("name", 1, 3)

# concatenate strings directly
F.concat("first_name", "last_name")

# concatenate strings with a separator
F.concat_ws(" ", "first_name", "last_name")

# split string into array using delimiter
F.split("email", "@")

# replace regex matches in a string
F.regexp_replace("text", r"\s+", " ")

# extract regex capture group from a string
F.regexp_extract("text", r"(\d+)", 1)

# find position of substring in string
F.instr("email", "@")

# example: clean a name by trimming and uppercasing
df = df.withColumn("name_clean", F.trim(F.upper("name")))

# =============================================================================
# NUMERIC FUNCTIONS
# =============================================================================

# absolute value
F.abs("amount")

# round to given number of decimal places
F.round("amount", 2)

# banker’s rounding to given decimal places
F.bround("amount", 2)

# smallest integer greater than or equal to value
F.ceil("amount")

# largest integer less than or equal to value
F.floor("amount")

# square root
F.sqrt("amount")

# return largest value among columns
F.greatest("a", "b", "c")

# return smallest value among columns
F.least("a", "b", "c")

# random number between 0 and 1
F.rand()

# example: create rounded numeric column
df = df.withColumn("amount_rounded", F.round("amount", 2))

# =============================================================================
# DATE AND TIMESTAMP FUNCTIONS
# =============================================================================

# current Spark date
F.current_date()

# current Spark timestamp
F.current_timestamp()

# parse string into date using given format
F.to_date("date_str", "yyyy-MM-dd")

# parse string into timestamp using given format
F.to_timestamp("ts_str", "yyyy-MM-dd HH:mm:ss")

# extract year from date/timestamp
F.year("dt")

# extract month number from date/timestamp
F.month("dt")

# extract day of month
F.dayofmonth("dt")

# extract day of week
F.dayofweek("dt")

# extract week number of year
F.weekofyear("dt")

# extract hour from timestamp
F.hour("ts")

# extract minute from timestamp
F.minute("ts")

# extract second from timestamp
F.second("ts")

# difference in days between two dates
F.datediff(F.current_date(), "dt")

# add days to a date
F.date_add("dt", 7)

# subtract days from a date
F.date_sub("dt", 7)

# add months to a date
F.add_months("dt", 1)

# number of months between two dates
F.months_between("end_date", "start_date")

# truncate date to month/year boundary
F.trunc("dt", "month")

# truncate timestamp to hour/day/month boundary
F.date_trunc("hour", "ts")

# get last day of month
F.last_day("dt")

# example: convert string column to date
df = df.withColumn("order_date", F.to_date("order_date_str", "yyyy-MM-dd"))

# =============================================================================
# NULL HANDLING AND CONDITIONAL LOGIC
# =============================================================================

# return first non-null value from arguments
F.coalesce("amount", F.lit(0))

# conditional logic like if/elif/else
F.when(F.col("score") >= 90, "A").when(F.col("score") >= 80, "B").otherwise("C")

# fill null values in selected columns
df.fillna({"amount": 0, "name": "unknown"})

# drop rows containing nulls
df.dropna()

# example: create label based on condition
df = df.withColumn("status", F.when(F.col("amount") > 1000, "high").otherwise("normal"))

# =============================================================================
# SORTING
# =============================================================================

# sort ascending by column
df.orderBy("name")

# sort descending by amount
df.orderBy(F.col("amount").desc())

# another sort example
df.sort(F.col("created_at").asc())

# =============================================================================
# AGGREGATION
# =============================================================================

# group rows and compute multiple aggregations
df.groupBy("dept").agg(
    F.count("*").alias("rows"),                    # count rows in each group
    F.countDistinct("emp_id").alias("uniq_emp"),  # count distinct values
    F.sum("salary").alias("sum_salary"),          # sum values
    F.avg("salary").alias("avg_salary"),          # average value
    F.min("salary").alias("min_salary"),          # minimum value
    F.max("salary").alias("max_salary")           # maximum value
)

# first value in group
F.first("col")

# last value in group
F.last("col")

# collect group values into array, keeping duplicates
F.collect_list("col")

# collect unique group values into array
F.collect_set("col")

# approximate distinct count, faster on large data
F.approx_count_distinct("col")

# =============================================================================
# DISTINCT AND DUPLICATE HANDLING
# =============================================================================

# remove duplicate rows across all columns
df.distinct()

# drop duplicate rows
df.dropDuplicates()

# drop duplicates based on subset of columns
df.dropDuplicates(["customer_id"])

# =============================================================================
# JOINS
# =============================================================================

# inner join keeps matching rows from both sides
df1.join(df2, on="id", how="inner")

# left join keeps all rows from left DataFrame
df1.join(df2, on="id", how="left")

# right join keeps all rows from right DataFrame
df1.join(df2, on="id", how="right")

# outer join keeps all rows from both sides
df1.join(df2, on="id", how="outer")

# broadcast small table to optimize join performance
df1.join(F.broadcast(df2), on="id", how="left")

# =============================================================================
# WINDOW FUNCTIONS
# =============================================================================

# define window partitioned by customer and ordered by date
w = Window.partitionBy("customer_id").orderBy("order_date")

# sequential row number within each window partition
F.row_number().over(w)

# rank with gaps for ties
F.rank().over(w)

# rank without gaps for ties
F.dense_rank().over(w)

# previous row's value within window
F.lag("amount", 1).over(w)

# next row's value within window
F.lead("amount", 1).over(w)

# example: add row number per customer ordered by date
df = df.withColumn("row_num", F.row_number().over(w))

# example: add previous amount for each row
df = df.withColumn("prev_amount", F.lag("amount", 1).over(w))

# define cumulative window from first row to current row
w2 = Window.partitionBy("customer_id").orderBy("order_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# running total over window
df = df.withColumn("running_total", F.sum("amount").over(w2))

# =============================================================================
# ARRAYS
# =============================================================================

# create an array column from multiple columns
F.array("c1", "c2", "c3")

# get number of elements in array
F.size("arr")

# check whether array contains a value
F.array_contains("arr", "x")

# sort array elements
F.sort_array("arr")

# remove duplicate values from array
F.array_distinct("arr")

# explode array into multiple rows
F.explode("arr")

# explode array into multiple rows and keep null/empty cases
F.explode_outer("arr")

# example: create array column
df = df.withColumn("items", F.array("item1", "item2"))

# example: explode array items into separate rows
df = df.withColumn("item", F.explode("items"))

# =============================================================================
# STRUCT, MAP, AND JSON
# =============================================================================

# create a struct from multiple columns
F.struct("first_name", "last_name")

# convert struct/map/array column to JSON string
F.to_json("struct_col")

# parse JSON string into struct using schema
F.from_json("json_col", schema)

# extract a field from JSON string using JSON path
F.get_json_object("json_col", "$.name")

# example: create struct column
df = df.withColumn("person", F.struct("first_name", "last_name"))

# example: convert struct column to JSON
df = df.withColumn("person_json", F.to_json("person"))

# =============================================================================
# EXPRESSIONS AND CASTING
# =============================================================================

# SQL-style expression
F.expr("price * qty")

# cast column to another type
F.col("amount").cast("double")

# cast column to timestamp type
F.col("created_at").cast("timestamp")

# example: compute total using SQL expression
df = df.withColumn("total", F.expr("price * qty"))

# =============================================================================
# READ DATA
# =============================================================================

# read CSV file
df = spark.read.csv("/path/file.csv", header=True, inferSchema=True)

# read Parquet file
df = spark.read.parquet("/path/file.parquet")

# read JSON file
df = spark.read.json("/path/file.json")

# read Spark table
df = spark.table("database.table_name")

# =============================================================================
# WRITE DATA
# =============================================================================

# write DataFrame as Parquet and overwrite target
df.write.mode("overwrite").parquet("/out/path")

# append rows into managed/external Spark table
df.write.mode("append").saveAsTable("my_table")

# write DataFrame as CSV with header
df.write.mode("overwrite").csv("/out/csv", header=True)

# write mode notes:
# overwrite = replace existing data
# append = add to existing data
# ignore = do nothing if target exists
# error / errorifexists = fail if target exists

# =============================================================================
# INSPECT DATA
# =============================================================================

# display top rows
df.show()

# display top rows without truncating long values
df.show(20, truncate=False)

# print DataFrame schema
df.printSchema()

# list column names
df.columns

# count total rows
df.count()

# show first 10 rows
df.limit(10).show()

# =============================================================================
# MOST USED FUNCTIONS
# =============================================================================

F.col               # reference a column
F.lit               # create a literal value
F.when              # conditional expression
F.coalesce          # first non-null value
F.upper             # uppercase string
F.lower             # lowercase string
F.trim              # trim spaces
F.current_timestamp # current timestamp
F.current_date      # current date
F.to_date           # parse to date
F.to_timestamp      # parse to timestamp
F.sum               # sum values
F.avg               # average values
F.count             # count rows/non-nulls
F.countDistinct     # count distinct values
F.min               # minimum
F.max               # maximum
F.row_number        # sequential row number in window
F.rank              # rank with gaps
F.lag               # previous value in window
F.lead              # next value in window
F.expr              # SQL expression
F.explode           # expand array into rows

# =============================================================================
# COMMON PATTERNS
# =============================================================================

# add current timestamp column
df = df.withColumn("created_at", F.current_timestamp())

# replace nulls with zero
df = df.withColumn("amount", F.coalesce("amount", F.lit(0)))

# label rows using condition
df = df.withColumn("status", F.when(F.col("amount") > 1000, "high").otherwise("normal"))

# group by category and aggregate
summary = df.groupBy("category").agg(
    F.count("*").alias("rows"),
    F.sum("sales").alias("total_sales")
)

# keep latest row per customer using row_number window
w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())
df_latest = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

# =============================================================================
# MINIMAL END-TO-END EXAMPLE
# =============================================================================

# read input data
df = spark.read.csv("/data/orders.csv", header=True, inferSchema=True)

# apply common transformations
df = (
    df.withColumn("created_at", F.current_timestamp())  # add load timestamp
      .withColumn("order_date", F.to_date("order_date_str", "yyyy-MM-dd"))  # parse date string
      .withColumn("customer_name", F.trim(F.upper("customer_name")))  # standardize text
      .withColumn("amount", F.coalesce("amount", F.lit(0)))  # replace null amount
      .withColumn("status", F.when(F.col("amount") > 1000, "high").otherwise("normal"))  # derive label
)

# summarize final dataset
summary = df.groupBy("status").agg(
    F.count("*").alias("rows"),           # number of rows per status
    F.avg("amount").alias("avg_amount")   # average amount per status
)

# display final result
summary.show()
