# Databricks notebook source
# DBTITLE 1,Functions
# MAGIC %run /Repos/frederick.ho@glasgow.ac.uk/CCU008/basic_func

# COMMAND ----------

# MAGIC %md # Cohort definition

# COMMAND ----------

# DBTITLE 1,Import curated demographic data
demo = spark.table('dsa_391419_j3w9t_collab.hds_curated_assets__demographics_2024_04_25')
demo = (
    demo
    .filter(F.col('in_gdppr')==1)
    .select(F.col('person_id').alias('pid'), 
            F.col('date_of_birth').alias('dob'), 
            'sex', 
            F.col('ethnicity_5_group').alias('ethnicity'), 
            F.col('imd_quintile').alias('imd5'), 
            F.col('date_of_death').alias('dod'), 
            'lsoa'
            )
    .withColumn('age', F.datediff(F.to_date(F.lit('2018-11-01')), F.col('dob')) / 365.25)
    .withColumn('age_gp', F.when(F.col('age') < 40, '18-39')
                         .when(F.col('age') < 60, '40-59')
                         .when(F.col('age') < 80, '60-79')
                         .otherwise('80+'))
    .withColumn('sex', F.when(F.col('sex').isNull(), 'I')
                        .otherwise(F.col('sex')))
    .withColumn('ethnicity', F.when(F.col('ethnicity').isNull() | (F.col('ethnicity')=='Other ethnic group'), 
                                    'Other/Unknown')
                              .otherwise(F.col('ethnicity')))
    .withColumn('area_code_prefix', F.substring(F.col("lsoa"), 0, 3))
)
demo = demo.filter(F.col('age') >= 18) 
demo = demo.filter((F.col('sex') != 'I') & F.col('sex').isNotNull())
demo = demo.filter((F.col('area_code_prefix')=='E01') & F.col('imd5').isNotNull())  

# COMMAND ----------

# DBTITLE 1,Import GP data
gp_all = spark.table('dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive')

# COMMAND ----------

# DBTITLE 1,Find start and end dates for individuals
popu = (
    gp_all.groupBy("NHS_NUMBER_DEID")
    .agg(F.min("REPORTING_PERIOD_END_DATE").alias("start_date"))
    .withColumn('start_date', F.date_sub("start_date", 731))
    .withColumnRenamed("NHS_NUMBER_DEID", "pid")
)

# COMMAND ----------

# DBTITLE 1,Join demographics and dates
demo = (
    demo.join(popu, ['pid'], how='inner')
    .withColumn("end_date", F.when(F.col("dod").isNull(), F.to_date(F.lit('2024-04-30'))) 
                             .otherwise(F.col("dod")))
    )

# COMMAND ----------

# DBTITLE 1,Excluding people with follow-up <1 month
demo =  demo.filter(F.months_between(F.col('end_date'), F.col('start_date')) >= 1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Stats

# COMMAND ----------

# DBTITLE 1,Tables
#demo.groupBy('age_gp').count().show()
#demo.groupBy('sex').count().show()
demo.groupBy('ethnicity').count().show()
demo.groupBy('imd5').count().show()

# COMMAND ----------

# DBTITLE 1,FU time
demo\
  .withColumn('FU_year', F.months_between(F.col("end_date"), F.col("start_date")) / 12)\
  .agg(
    F.percentile_approx('FU_year', 0.50, 500).alias('median'), 
    F.percentile_approx('FU_year', 0.25, 500).alias('Q1') , 
    F.percentile_approx('FU_year', 0.75, 500).alias('Q3')
  )\
  .show()
