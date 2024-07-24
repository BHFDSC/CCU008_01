# Databricks notebook source
# Let's import some libraries and packages
import pyspark.sql.functions as F
import pyspark.sql.types as T
import sys
from pyspark.sql.window import Window
from pyspark.sql import Row
from datetime import date


# COMMAND ----------

# MAGIC %run /Shared/SHDS/Mehrdad/helper_library/basic_functions

# COMMAND ----------

# %run /Workspaces/dars_nic_391419_j3w9t_collab/SHDS/library/outputcheck/suppression/suppression_functions_beta

# COMMAND ----------

def cal_age(df, dom, dob):
  df = df.withColumn('age', F.months_between(F.to_date(dom),  F.to_date(F.concat(dob, F.lit('-15'))))/12)
  df = df.filter(df.age>=18).\
          withColumn('age_gp', F.when(df.age < 40, '18-39').\
                                 when(df.age < 60, '40-59').\
                                 when(df.age < 80, '60-79').\
                                 otherwise('80+')).\
          drop(dob)
  return(df)

# COMMAND ----------

def clean_outlier(df, k):
  QT = df.approxQuantile('value1', [0.25, 0.75], 0.001)
  print(f'''Lower quartile = {QT[0]}; Upper quartile = {QT[1]}''')
  df = df.\
    filter((F.col('value1') >= QT[0] - k*(QT[1] - QT[0])) & 
           (F.col('value1') <= QT[1] + k*(QT[1] - QT[0])))
  return(df)

# COMMAND ----------

def select_cohort_trend(df):
  w1 = Window.partitionBy("pid").orderBy(F.col('date'))

  df_sd = df.\
    withColumn('row', F.row_number().over(w1)).filter(F.col('row')==1).\
    select('pid', 'age', 'age_gp', 'sex', 'imd19_q10', 'value1')

  df1 = df.filter(F.col('date') <= date(2020, 3, 26)).orderBy(['pid', 'date'])
  df1 = df1.withColumn("first_date", F.min("date").over(w1)).withColumn("last_date", F.max("date").over(w1))
  df1 = df1.filter(F.col('date')==F.col('first_date')).select('pid', 'first_date', 'value1').withColumnRenamed('value1', 'first_value').\
    join(df1.filter(F.col('date')==F.col('last_date')).select('pid', 'last_date', 'value1').withColumnRenamed('value1', 'last_value'), 
         ['pid'], 'inner')
  df1 = df1.\
    withColumn('val_d1', F.col('last_value') - F.col('first_value')).\
    withColumn('date_d1', F.datediff(F.col('last_date'), F.col('first_date'))).\
    groupBy('pid').agg(F.mean(F.col('date_d1')).alias('date_d1'), F.mean(F.col('val_d1')).alias('val_d1')).\
    filter(F.col('date_d1') >= 90).\
    withColumn('val_dy1', F.col('val_d1') / (F.col('date_d1')/365.25))

  df2 = df.filter(F.col('date') >  date(2020, 3, 26)).orderBy(['pid', 'date'])
  df2 = df2.withColumn("first_date", F.min("date").over(w1)).withColumn("last_date", F.max("date").over(w1))
  df2 = df2.filter(F.col('date')==F.col('first_date')).select('pid', 'first_date', 'value1').withColumnRenamed('value1', 'first_value').\
    join(df2.filter(F.col('date')==F.col('last_date')).select('pid', 'last_date', 'value1').withColumnRenamed('value1', 'last_value'), 
         ['pid'], 'inner')
  df2 = df2.\
    withColumn('val_d2', F.col('last_value') - F.col('first_value')).\
    withColumn('date_d2', F.datediff(F.col('last_date'), F.col('first_date'))).\
    groupBy('pid').agg(F.mean(F.col('date_d2')).alias('date_d2'), F.mean(F.col('val_d2')).alias('val_d2')).\
    filter(F.col('date_d2') >= 90).\
    withColumn('val_dy2', F.col('val_d2') / (F.col('date_d2')/365.25))


  df = df_sd.join(df1, ['pid'], 'inner').join(df2, ['pid'], 'inner').orderBy('pid')
  return(df)

# COMMAND ----------

def select_cohort(df):
  df_id = df.\
    filter(F.col('yod').isNull()).\
    groupBy(F.col('pid')).\
    agg(F.min('date').alias('min_date'), F.max('date').alias('max_date')).\
    filter((F.col('min_date') < date(2020, 3, 26)) & 
           (F.col('max_date') > date(2020, 3, 26))).\
    select('pid')
  
  df = df_id.join(df, ['pid'], 'inner')
  
  return(df)

# COMMAND ----------

def sample_cohort(df, rf, p):
  df = df.filter(F.col('yod').isNull())
  df = df.filter(F.col('mst')==rf).\
    join(df.filter(F.col('mst')==rf).select('pid').distinct().sample(p, 12345), ['pid'], 'right')
  
  return(df)

# COMMAND ----------

def descr(df, mst):
  df = df.\
    groupBy(F.year('date').alias('year')).\
    agg(F.count('*').alias('n'), 
        F.avg('value1').alias('mean'))
  df = df.\
    sort('year').\
    withColumn('mst', F.lit(mst)).\
    filter(F.col('n')>=10)
  return(df)

# COMMAND ----------

def descr_sub(df, mst):
  df = df.\
    groupBy(F.year('date').alias('year'), 
            F.month('date').alias('month'), 
            'sex', 'age_gp', 'imd19_q10').\
    agg(F.count('pid').alias('n'), 
        F.countDistinct('pid').alias('n_id'), 
        F.avg('value1').alias('mean'))
  df = df.\
    sort('year', 'month', 'sex', 'age_gp', 'imd19_q10').\
    withColumn('mst', F.lit(mst)).\
    filter(F.col('n')>=10)
  return(df)

# COMMAND ----------

def descr_sub(df, mst):
  df = df.\
    groupBy(F.year('date').alias('year'), 
            F.month('date').alias('month'), 
            'sex', 'age_gp', 'imd19_q10').\
    agg(F.count('pid').alias('n'), 
        F.countDistinct('pid').alias('n_id'), 
        F.avg('value1').alias('mean'))
  df = df.\
    sort('year', 'month', 'sex', 'age_gp', 'imd19_q10').\
    withColumn('mst', F.lit(mst)).\
    filter(F.col('n')>=10)
  return(df)
