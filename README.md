# PySpark CheatSheet

## Set Up
```
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q http://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
!tar xf spark-3.1.1-bin-hadoop3.2.tgz
!pip install -q findspark
```

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.1-bin-hadoop3.2"
```

```python
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # Property used to format output tables better
```

## Read CSV
```python
df = spark.read.csv('cars.csv', header=True, sep=";")
df = spark.read.csv('cars.csv', header=True, sep=";", inferSchema=True)
```

## Show first 5 values
```python
df.show(5, truncate=False) 
```
> **Note:** The `truncate=True` truncate strings longer than 20 chars by default.

## Show Schema
```python
df.printSchema()
df.dtypes
```

## Define Schema
```python
from pyspark.sql.types import *
df.columns
# Creating a list of the schema in the format column_name, data_type
labels = [
('Car',StringType()),
('MPG',DoubleType()),
('Cylinders',IntegerType()),
('Displacement',DoubleType()),
('Horsepower',DoubleType()),
('Weight',DoubleType()),
('Acceleration',DoubleType()),
('Model',IntegerType()),
('Origin',StringType())
]
# Creating the schema that will be passed when reading the csv
schema = StructType([StructField (x[0], x[1],  True)  for x in labels])
df = spark.read.csv('cars.csv', header=True, sep=";", schema=schema)
```
## Select Column/s
```python
df.select(df['car'],df['cylinders']).show(truncate=False)
```
## Adding New Columns
```python
# We will add two new columns called 'second_column' and 'third_column' at the end
df = df.withColumn('second_column', lit(2)) \
.withColumn('third_column', lit('Third Column'))
# lit means literal. It populates the row with the literal value given.
# When adding static data / constant values, it is a good practice to use it.
```
## Renaming Columns
```python
df = df.withColumnRenamed('first_column',  'new_column_one') \
.withColumnRenamed('second_column',  'new_column_two') \
.withColumnRenamed('third_column',  'new_column_three')
```
## Transformations
```python
df = df.drop('new_column_one') # Drop column
df.groupBy('Origin').count().show(5) # Group by
df.filter(col('Origin')=='Europe').show(truncate=False) # Filter
```

## Unions
```python
# It is used to merge two DataFrames of the same structure/schema.
europe_cars.union(japan_cars).show()
# This function is used to merge two dataframes based on column name.
df1.unionByName(df2).show()
```

## Joins
```python
cars_df = spark.createDataFrame([[1, 'Car A'],[2, 'Car B'],[3, 'Car C']], ["id", "car_name"])
car_price_df = spark.createDataFrame([[1, 1000],[2, 2000],[3, 3000]], ["id", "car_price"])
# Executing an inner join so we can see the id, name and price of each car in one row
cars_df.join(car_price_df, cars_df.id == car_price_df.id, 'inner').select(cars_df['id'],cars_df['car_name'],car_price_df['car_price']).show(truncate=False)
```

## SQL
```python
# Load data
df = spark.read.csv('cars.csv', header=True, sep=";")
# Register Temporary Table
df.createOrReplaceTempView("temp")
# Select all data from temp table
spark.sql("select * from temp limit 5").show()
```

## Drop Duplicates
```python
from pyspark.sql import Row
from pyspark.sql import Row
mylist = [
  {"name":'Alice',"age":5,"height":80},
  {"name":'Jacob',"age":24,"height":80},
  {"name":'Alice',"age":5,"height":80}
]
df = spark.createDataFrame(Row(**x) for x in mylist)
df.dropDuplicates().show()
```

## Best Practices
Try to incorporate these to your coding habits for better performance:
1.   Do not use `NOT IN` use `NOT EXISTS`.
2.   Remove `Counts`, `Distinct Counts` (use `approxCountDIstinct`).
3.   Drop Duplicates early.
4.   Always prefer SQL functions over PandasUDF.
5.   Use Hive partitions effectively.
6.   Leverage Spark UI effectively.
7.   Avoid Shuffle Spills.
8.   Aim for target cluster utilization of at least 70%.

> **Note:** [Documentation](https://colab.research.google.com/drive/1G894WS7ltIUTusWWmsCnF_zQhQqZCDOc)
