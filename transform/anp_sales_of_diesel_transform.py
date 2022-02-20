from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from datetime import datetime

# Declaration of variables
job_name = 'Transform ANP Fuel Sales'
today = datetime.now().strftime("%Y/%m/%d")
path_load_s3 = f"s3://''/raw/anp_fuel_sales/{today}/sales_of_diesel.parquet"
path_write_s3 = f"s3://''/transform/anp_fuel_sales/sales_of_diesel/"

# Set SparkSession configuration
spark = (
      SparkSession.builder 
     .master("yarn") 
     .appName(job_name) 
     .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
     .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") 
     .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
     .getOrCreate()
     )
#Reads parquet files from the raw directory on s3
df = spark.read.format("parquet").load(path_load_s3)
df = df.drop('TOTAL')

# Defines the columns that will not be unpivoted
fixed_columns = ['combustivel','ano','regiao','estado']

#Convert column names to lowercase
for col in df.columns:
    df = df.withColumnRenamed(col, (col).lower())

# Creates the expression to apply unpivot to columns that do not belong to the fixed column list. Performs the count of columns to be converted.
def unpivot_dataframe(): 
    expression = ""
    count=0
    for column in df.columns:
        if column not in fixed_columns:
            count +=1
            expression += f"'{column}' , {column},"
    expression = f"stack({count}, {expression[:-1]}) as (mes,volume)"
    return expression
expression = unpivot_dataframe()

# Select the variables by passing the expression created in the unpivot_dataframe function
df = df.select('combustivel','ano','regiao','estado',F.expr(expression))

#Apply the transformations and creation of the columns requested in the challenge
df = (
        df.withColumn('mes', F.when(F.col('mes') == 'jan',1)
                           .when(F.col('mes') == 'fev',2)
                           .when(F.col('mes') == 'mar',3)
                           .when(F.col('mes') == 'abr',4)
                           .when(F.col('mes') == 'mai',5)
                           .when(F.col('mes') == 'jun',6)
                           .when(F.col('mes') == 'jul',7)
                           .when(F.col('mes') == 'ago',8)
                           .when(F.col('mes') == 'set',9)
                           .when(F.col('mes') == 'out',10)
                           .when(F.col('mes') == 'nov',11)
                           .when(F.col('mes') == 'dez',12))
       .withColumn('created_at', F.current_timestamp())
       .withColumn('uf', F.col('estado'))
       .withColumn('product', F.col('combustivel'))
       .withColumn('year_month', F.concat(F.col('ano'),F.lit('-'),F.col('mes')))
       .withColumn('unit', F.lit('m3'))
       .withColumn('year_month', F.to_date(F.col('year_month'), 'yyyy-M'))
       .select(F.col('year_month'),
               F.col('uf'),
               F.col('product'),
               F.col('unit'),
               F.col('volume'),
               F.col('created_at'))
)
# Write dataframe to s3
df.write.mode('overwrite').parquet(path_write_s3)
