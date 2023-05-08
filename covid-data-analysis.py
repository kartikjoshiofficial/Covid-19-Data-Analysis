#importing ilibraries

import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import os
for dirname, _, filenames in os.walk('dataset.xslx'):
    for filename in filenames:
        print(os.path.join(dirname, filename))



#Reading the Data

!pip install pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from tqdm import tqdm
import pandas as pd

def load_raw():
    #Create Spark Session
    spark = SparkSession.builder \
        .master('local') \
        .appName('myAppName') \
        .config('spark.executor.memory', '12gb') \
        .config("spark.cores.max", "10") \
        .getOrCreate()

    #Get spark context
    sc = spark.sparkContext


    sqlContext = SQLContext(sc)
    
    df=pd.read_excel('dataset.xslx')
    
    
    df['Respiratory Syncytial Virus']=df['Respiratory Syncytial Virus'].astype(str)
    df['Influenza A']=df['Influenza A'].astype(str)
    df['Influenza B']=df['Influenza B'].astype(str)
    df['Parainfluenza 1']=df['Parainfluenza 1'].astype(str)
    df['CoronavirusNL63']=df['CoronavirusNL63'].astype(str)
    df['Rhinovirus/Enterovirus']=df['Rhinovirus/Enterovirus'].astype(str)
    df['Coronavirus HKU1']=df['Coronavirus HKU1'].astype(str)
    
    for column in df.columns:
        df[column]=df[column].astype(str)
    
    df=sqlContext.createDataFrame(df)
    return df,sqlContext


df,sqlContext=load_raw()

print('Number of lines ',df.count())

#print schema
df.printSchema()

#print data
df=df.fillna(0)
from pyspark.sql.functions import *
df=df.replace("nan", "0")
pd.DataFrame(df.head(5),columns=df.schema.names)

#Visualizing dta using pandas
df_hemoglobin=df.select("Hemoglobin").toPandas()
df_hemoglobin['Hemoglobin']=pd.to_numeric(df_hemoglobin['Hemoglobin'])
df_hemoglobin['Hemoglobin'].hist()

df.select("SARS-Cov-2 exam result").show()

df_=df.select(col("SARS-Cov-2 exam result").alias("result"),col('Patient age quantile').alias('age'))
df_.show()

#Save Data into a Parquet Format
df_.select("result", "age").write.mode('overwrite').option("header", "true").save("result_age.parquet", format="parquet")
df_ = sqlContext.sql("SELECT * FROM parquet.`result_age.parquet`")
df_.printSchema()

import pyspark.sql.functions as func

#Aggregation Funcions
df_.groupBy("result").agg(func.max("age"), func.avg("age")).show()
df_pandas_age=df_.groupBy("result").agg(func.max("age"), func.avg("age")).toPandas()
df_pandas_age.plot()

from pyspark.sql.types import IntegerType
columns=df.schema.names
for column in columns:
    df= df.withColumn(column, df[column].cast(IntegerType()))

df.printSchema()
df.shoe()
