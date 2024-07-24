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
print(f'In GDPPR, n= {demo.count()}')

demo = demo.filter(F.col('age') >= 18) 
print(f'Adults at Nov 2018, n= {demo.count()}')

demo = demo.filter((F.col('sex') != 'I') & F.col('sex').isNotNull())
print(f'Known sex, n= {demo.count()}')

demo = demo.filter((F.col('area_code_prefix')=='E01') & F.col('imd5').isNotNull())  
print(f'Valid address in England only, n= {demo.count()}')

# COMMAND ----------

# DBTITLE 1,Import GP data
gp_all = spark.table('dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive')
#print(f'GP record count: {gp_all.count()}')
#print(gp_all.agg({"ProductionDate": "max"}).first()[0])


# COMMAND ----------

# DBTITLE 1,Find start and end dates for individuals
popu = (
    gp_all.groupBy("NHS_NUMBER_DEID")
    .agg(F.min("REPORTING_PERIOD_END_DATE").alias("start_date"))
    .withColumn('start_date', F.date_sub("start_date", 731))
    .withColumnRenamed("NHS_NUMBER_DEID", "pid")
)
#print(f'Popu record count: {popu.count()}')

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
print(f'With at least 1 month follow-up, n= {demo.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Population count

# COMMAND ----------

# DBTITLE 1,Create multiple rows by eligible months
pop_cnt = (
    demo
    .withColumn("date", F.explode(F.sequence('start_date', 'end_date', F.expr('interval 1 month'))))
)
#print(f'Row: {pop_cnt.count()}')

# COMMAND ----------

# DBTITLE 1,Count monthly population size
pop_cnt = (
  pop_cnt
  .withColumn('age', F.datediff(F.col('date'), F.col('dob')) / 365.25)
  .withColumn('age_gp', F.when(F.col('age') < 40, '18-39')
                         .when(F.col('age') < 60, '40-59')
                         .when(F.col('age') < 80, '60-79')
                         .otherwise('80+'))
  .select('age_gp', 'sex', 'ethnicity', 'imd5', 
          F.month(F.col('date')).alias('month'), 
          F.year(F.col('date')).alias('year'))
  .groupBy('year', 'month', 'age_gp', 'sex', 'ethnicity', 'imd5')
  .count()
)
#print(f'Row: {pop_cnt.count()}')

# COMMAND ----------

# DBTITLE 1,Save pouplation count
pop_cnt.write.mode("overwrite").saveAsTable("dsa_391419_j3w9t_collab.ccu008_popu_cnt_20240703D")

# COMMAND ----------

# MAGIC %md # Risk factor count

# COMMAND ----------

# DBTITLE 1,Import codelist
# MAGIC %run /Repos/frederick.ho@glasgow.ac.uk/CCU008/Codelist_v2

# COMMAND ----------

# DBTITLE 1,Join with codelist and demo
rf = (
    gp_all
    .filter(F.col("ProductionDate").startswith("2024-05-28")) # latest version
    .select(F.col('NHS_NUMBER_DEID').alias('pid'), 
            F.col('DATE').alias('date'), 
            F.col('CODE').alias('ConceptId'), 
            F.col('VALUE1_CONDITION').alias('value1'))
    .join(codelist_sel.select('rf', 'ConceptId'), ['ConceptId'], how='inner')  # join with selected codelist
    .filter((F.col('value1').isNotNull() | F.col('rf').isin(['Smoking', 'Alcohol'])))  # contains at least 1 value except for smoking and alcohol 
    .join(demo, ['pid'], how='inner')
    .filter((F.col('date') >= F.col('start_date')) & (F.col('date') <= F.col('end_date'))) # only within eligible time
    .select('pid', 'rf', 'date',
            F.year(F.col('DATE')).alias('year'), 
            F.month(F.col('DATE')).alias('month'), 
            (F.datediff(F.col('date'), F.col('dob')) / 365.25).alias('age'), 
            F.when(F.col('age') < 40, '18-39')
              .when(F.col('age') < 60, '40-59')
              .when(F.col('age') < 80, '60-79')
              .otherwise('80+')
              .alias('age_gp'), 
            'sex', 'ethnicity', 'imd5')
    .drop('age')
)
rf = rf.dropDuplicates() # remove multiple entries in the same date
#print(f'GP record count: {rf.count()}')

# COMMAND ----------

# DBTITLE 1,Count monthly RF
rf_cnt = (
        rf.drop('pid', 'date')
          .groupBy('rf', 'year', 'month', 'age_gp', 'sex', 'ethnicity', 'imd5')
          .count()
)

# COMMAND ----------

# DBTITLE 1,Save RF count
rf_cnt.write.mode("overwrite").saveAsTable("dsa_391419_j3w9t_collab.ccu008_rf_cnt_20240710")
