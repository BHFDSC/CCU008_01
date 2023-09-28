# Databricks notebook source
# MAGIC %run /Repos/frederick.ho@glasgow.ac.uk/CCU008/basic_func

# COMMAND ----------

# MAGIC %md # Import from raw

# COMMAND ----------

gp_raw = spark.table('dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive')
#print(gp_raw.agg({"ProductionDate": "max"}).first()[0])

# COMMAND ----------

gp = gp_raw.\
  filter((F.col("ProductionDate").startswith("2023-05-23")) & 
         (F.col("DATE") >= date(2015,2,1)) & 
         (F.col("DATE") <= date.today()) & 
         (F.col("DATE").isNotNull()) & 
         ((F.col('SEX') == 1) |(F.col('SEX') == 2))).\
  select(F.col("NHS_NUMBER_DEID").alias('pid'), 
         F.col("YEAR_MONTH_OF_BIRTH").alias('dob'), 
         F.col("SEX").alias('sex'), 
         F.col('LSOA').alias('LSOA'),
         F.col("YEAR_OF_DEATH").alias('yod'), 
         F.col("DATE").alias('date'), 
         F.col('CODE').alias('code'), 
         F.col("VALUE1_CONDITION").alias('value1'), 
         F.col("VALUE2_CONDITION").alias('value2'))
print(f'record count = {gp.count()}')
print(f'patient count = {gp.select(F.col("pid")).distinct().count()}')

# COMMAND ----------

# only including LSOA in England 
gp = gp.\
  withColumn('area_code_prefix', F.substring(F.col("LSOA"), 0, 3)).\
  filter(F.col('area_code_prefix')=='E01').\
  drop('area_code_prefix')

# COMMAND ----------

# Create an IMD-2019 look up table by LSOA code
lsoa_imd_lookup = spark.table('dss_corporate.english_indices_of_dep_v02').\
  filter(F.col('IMD_YEAR') == 2019).\
    withColumnRenamed('LSOA_CODE_2011', 'LSOA').\
    withColumn('imd19_q5',\
      F.when(F.col('DECI_IMD').isin([1,2]), 1).\
        when(F.col('DECI_IMD').isin([3,4]), 2).\
        when(F.col('DECI_IMD').isin([5,6]), 3).\
        when(F.col('DECI_IMD').isin([7,8]), 4).\
        when(F.col('DECI_IMD').isin([9,10]), 5).\
        otherwise(None)\
    ).\
    withColumnRenamed('DECI_IMD', 'imd19_q10').\
    select('LSOA', 'imd19_q5', 'imd19_q10')

gp = gp.join(lsoa_imd_lookup, ['LSOA'], how='inner').drop('LSOA')

# COMMAND ----------

# MAGIC %md # Extract risk factors

# COMMAND ----------

# MAGIC %md ## Behavioural

# COMMAND ----------

# Snomed Codes for BMI with a value

code_bmi = [
'60621009',
'846931000000101'
]

code_bmi = sc.broadcast(code_bmi)

gp_bmi = gp.filter(F.col('code').isin(code_bmi.value) & F.col('value1').isNotNull()).drop('value2')


#check 
#print(f'bmi record count = {gp_bmi.count()}')
#gp_bmi.groupBy('code').count().show()

# COMMAND ----------

# Snomed Codes for smoking (cig/day)

code_cig = [
'230056004',
'266918002'
]

code_cig = sc.broadcast(code_cig)

gp_cig = gp.filter(F.col('code').isin(code_cig.value) & F.col('value1').isNotNull()).drop('value2')

#check 
#print(f'smoking unit record count = {gp_cig.count()}')
#gp_alco.groupBy('code').count().show()

# COMMAND ----------

# Snomed Codes for problem drinkers

code_alco = [
'1082631000000102',
'1082641000000106', 
'160573003'
]

code_alco = sc.broadcast(code_alco)

gp_alco = gp.filter(F.col('code').isin(code_alco.value) & F.col('value1').isNotNull()).drop('value2')

#check 
#print(f'alcohol unit record count = {gp_alco.count()}')


# COMMAND ----------

# MAGIC %md ## Metabolic

# COMMAND ----------

# Snomed Codes for BP

code_bp = [
'1091811000000102',
'163035008',
'174255007',
'251070002',
'251076008',
'271649006',
'271650006',
'364090009',
'386534000',
'386536003',
'72313002',
'723237002',
'75367002',
'399304008',
'400974009', 
'400975005',
'407555005', 
'407554009'
]

code_bp = sc.broadcast(code_bp)

gp_bp = gp.filter(F.col('code').isin(code_bp.value) & F.col('value1').isNotNull())

#check 
#print(f'BP record count = {gp_bp.count()}')


# COMMAND ----------

# Snomed Codes for fasting glucose 

code_fg = [
'1003141000000105',
'1003131000000101',
'997681000000108'
]

code_fg = sc.broadcast(code_fg)

gp_fg = gp.filter(F.col('code').isin(code_fg.value) & F.col('value1').isNotNull()).drop('value2')

#check 
#print(f'fasting glucose record count = {gp_fg.count()}')


# COMMAND ----------

# Snomed Codes for HbA1c

code_a1c = [
'999791000000106'
]

code_a1c = sc.broadcast(code_a1c)

gp_a1c = gp.filter(F.col('code').isin(code_a1c.value) & F.col('value1').isNotNull()).drop('value2')

#check 
#print(f'HbA1c units record count = {gp_a1c.count()}')


# COMMAND ----------

# MAGIC %md ## Lipid

# COMMAND ----------

# Snomed Codes for total cholestrol with a value

code_chol = [
'1005671000000105', 
'1017161000000104',
'1083761000000106',
'1106531000000105',
'1106541000000101',
'994351000000103',
'850981000000101',
'853681000000104',
]

code_chol = sc.broadcast(code_chol)

gp_chol = gp.filter(F.col('code').isin(code_chol.value) & F.col('value1').isNotNull()).drop('value2')

#check 
#print(f'chol record count = {gp_chol.count()}')
#gp_chol.groupBy('code').count().show()

# COMMAND ----------

# Snomed Codes for HDL cholestrol with a value (test of LIKE opera)

code_hdl = [
'1005681000000107',
'1010581000000101',
'1026451000000102',
'1026461000000104',
'1028831000000106',
'1028841000000102',
'1107661000000104',
'1107681000000108',
]

code_hdl = sc.broadcast(code_hdl)

gp_hdl = gp.filter(F.col('code').isin(code_hdl.value) & F.col('value1').isNotNull()).drop('value2')

#check 
#print(f'HDL record count = {gp_hdl.count()}')
#gp_hdl.groupBy('code').count().show()

# COMMAND ----------

# Snomed Codes for LDL cholestrol with a value

code_ldl = [
'1010591000000104',
'1022191000000100',
'1026471000000106',
'1026481000000108',
'1028851000000104',
'1028861000000101',
'1108541000000100',
'1108551000000102',
'1014501000000104'
]


code_ldl = sc.broadcast(code_ldl)

gp_ldl = gp.filter(F.col('code').isin(code_ldl.value) & F.col('value1').isNotNull()).drop('value2')

#check 
#print(f'LDL record count = {gp_ldl.count()}')
#gp_ldl.groupBy('code').count().show()

# COMMAND ----------

# Snomed Codes for fasting triglyceride with a value

code_tg = [
'1005691000000109',
'1010601000000105',
'1026491000000105',
'1026501000000104',
'1028871000000108',
'1031321000000109'
'1109821000000101',
'1109831000000104',
'850991000000104', 

]

code_tg = sc.broadcast(code_tg)

gp_tg = gp.filter(F.col('code').isin(code_tg.value) & F.col('value1').isNotNull()).drop('value2')

#check 
#print(f'TG record count = {gp_tg.count()}')
#gp_tg.groupBy('code').count().show()

# COMMAND ----------

# MAGIC %md ## Liver

# COMMAND ----------

# Snomed Codes for AST with a value

code_ast = [
'1000881000000102',
'1000891000000100',
'1031101000000102',
'1106071000000102',
'1106681000000105'
]

code_ast = sc.broadcast(code_ast)

gp_ast = gp.filter(F.col('code').isin(code_ast.value) & F.col('value1').isNotNull()).drop('value2')


#check 
#print(f'AST record count = {gp_ast.count()}')
#gp_ast.groupBy('code').count().show()

# COMMAND ----------

# Snomed Codes for ALT with a value

code_alt = [
'1013211000000103',
'1018251000000107',
'1105851000000108',
'1106081000000100'
]

code_alt = sc.broadcast(code_alt)

gp_alt = gp.filter(F.col('code').isin(code_alt.value) & F.col('value1').isNotNull()).drop('value2')


#check 
#print(f'ALT record count = {gp_alt.count()}')
#dppr_alt.groupBy('code').count().show()

# COMMAND ----------

# Snomed Codes for GGT with a value

code_ggt = [
'1000851000000108',
'1000861000000106',
'1028091000000102',
'1107431000000107',
'1107441000000103'
]

code_ggt = sc.broadcast(code_ggt)

gp_ggt = gp.filter(F.col('code').isin(code_ggt.value) & F.col('value1').isNotNull()).drop('value2')


#check 
#print(f'GGT record count = {gp_ggt.count()}')
#gp_ggt.groupBy('code').count().show()

# COMMAND ----------

# MAGIC %md # Data cleaning

# COMMAND ----------

code_bp = [
'163035008',
'251076008',
'364090009',
'386534000',
'386536003',
'723237002',
'75367002']

code_dbp = [
'1091811000000102',
'174255007',
'271650006',
'400975005'
'407555005']

code_sbp = [
'251070002',
'271649006',
'72313002',
'399304008',
'400974009', 
'407554009'
]

gp_sbp = gp_bp.filter(F.col('CODE').isin(code_sbp)).drop('value2').\
  union(gp_bp.filter(F.col('CODE').isin(code_bp)).drop('value2'))

gp_dbp = gp_bp.filter(F.col('CODE').isin(code_dbp)).drop('value2').\
  union(gp_bp.filter(F.col('CODE').isin(code_bp)).drop('value1').withColumnRenamed('value2', 'value1'))

# COMMAND ----------

gp_bmi   = clean_outlier(cal_age(gp_bmi  , 'date', 'dob'), 10)
gp_cig   = clean_outlier(cal_age(gp_cig  , 'date', 'dob'), 10)
gp_alco  = clean_outlier(cal_age(gp_alco , 'date', 'dob'), 10)
gp_sbp   = clean_outlier(cal_age(gp_sbp  , 'date', 'dob'), 10)
gp_dbp   = clean_outlier(cal_age(gp_dbp  , 'date', 'dob'), 10)
gp_fg    = clean_outlier(cal_age(gp_fg   , 'date', 'dob'), 10)
gp_a1c   = clean_outlier(cal_age(gp_a1c  , 'date', 'dob'), 10)
gp_chol  = clean_outlier(cal_age(gp_chol , 'date', 'dob'), 10)
gp_hdl   = clean_outlier(cal_age(gp_hdl  , 'date', 'dob'), 10)
gp_ldl   = clean_outlier(cal_age(gp_ldl  , 'date', 'dob'), 10)
gp_tg    = clean_outlier(cal_age(gp_tg   , 'date', 'dob'), 10)
gp_ast   = clean_outlier(cal_age(gp_ast  , 'date', 'dob'), 10)
gp_alt   = clean_outlier(cal_age(gp_alt  , 'date', 'dob'), 10)
gp_ggt   = clean_outlier(cal_age(gp_ggt  , 'date', 'dob'), 10)

# COMMAND ----------

# MAGIC %md # Export population data

# COMMAND ----------

save_checkpoint(df_in=gp_bmi  , database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_bmi_fh_160623"  , global_view_name="ccu008")
save_checkpoint(df_in=gp_cig  , database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_cig_fh_160623"  , global_view_name="ccu008")
save_checkpoint(df_in=gp_alco , database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_alco_fh_160623" , global_view_name="ccu008")
save_checkpoint(df_in=gp_sbp  , database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_sbp_fh_160623"  , global_view_name="ccu008")
save_checkpoint(df_in=gp_dbp  , database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_dbp_fh_160623"  , global_view_name="ccu008")
save_checkpoint(df_in=gp_fg   , database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_fg_fh_160623"   , global_view_name="ccu008")
save_checkpoint(df_in=gp_a1c  , database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_a1c_fh_160623"  , global_view_name="ccu008")
save_checkpoint(df_in=gp_chol , database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_chol_fh_160623" , global_view_name="ccu008")
save_checkpoint(df_in=gp_hdl  , database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_hdl_fh_160623"  , global_view_name="ccu008")
save_checkpoint(df_in=gp_ldl  , database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_ldl_fh_160623"  , global_view_name="ccu008")
save_checkpoint(df_in=gp_tg   , database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_tg_fh_160623"   , global_view_name="ccu008")
save_checkpoint(df_in=gp_ast  , database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_ast_fh_160623"  , global_view_name="ccu008")
save_checkpoint(df_in=gp_alt  , database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_alt_fh_160623"  , global_view_name="ccu008")
save_checkpoint(df_in=gp_ggt  , database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_ggt_fh_160623"  , global_view_name="ccu008")

# COMMAND ----------

# MAGIC %md # Export cohort data

# COMMAND ----------

"""save_checkpoint(df_in=select_cohort(gp_bmi  ), database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_coh_bmi_fh_160623"  , global_view_name="ccu008")
save_checkpoint(df_in=select_cohort(gp_cig  ), database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_coh_cig_fh_160623"  , global_view_name="ccu008")
save_checkpoint(df_in=select_cohort(gp_alco ), database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_coh_alco_fh_160623" , global_view_name="ccu008")
save_checkpoint(df_in=select_cohort(gp_sbp  ), database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_coh_sbp_fh_160623"  , global_view_name="ccu008")
save_checkpoint(df_in=select_cohort(gp_dbp  ), database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_coh_dbp_fh_160623"  , global_view_name="ccu008")
save_checkpoint(df_in=select_cohort(gp_fg   ), database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_coh_fg_fh_160623"   , global_view_name="ccu008")
save_checkpoint(df_in=select_cohort(gp_a1c  ), database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_coh_a1c_fh_160623"  , global_view_name="ccu008")
save_checkpoint(df_in=select_cohort(gp_chol ), database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_coh_chol_fh_160623" , global_view_name="ccu008")
save_checkpoint(df_in=select_cohort(gp_hdl  ), database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_coh_hdl_fh_160623"  , global_view_name="ccu008")
save_checkpoint(df_in=select_cohort(gp_ldl  ), database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_coh_ldl_fh_160623"  , global_view_name="ccu008")
save_checkpoint(df_in=select_cohort(gp_tg   ), database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_coh_tg_fh_160623"   , global_view_name="ccu008")
save_checkpoint(df_in=select_cohort(gp_ast  ), database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_coh_ast_fh_160623"  , global_view_name="ccu008")
save_checkpoint(df_in=select_cohort(gp_alt  ), database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_coh_alt_fh_160623"  , global_view_name="ccu008")
save_checkpoint(df_in=select_cohort(gp_ggt  ), database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_coh_ggt_fh_160623"  , global_view_name="ccu008")"""

# COMMAND ----------

# MAGIC %md # Remove tables

# COMMAND ----------

drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_cig_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_alco_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_sbp_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_dbp_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_fg_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_a1c_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_chol_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_hdl_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_ldl_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_tg_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_ast_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_alt_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_pop_ggt_fh_020323")

drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_ca2_bmi_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_ca2_cig_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_ca2_alco_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_ca2_sbp_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_ca2_dbp_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_ca2_fg_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_ca2_a1c_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_ca2_chol_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_ca2_hdl_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_ca2_ldl_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_ca2_tg_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_ca2_ast_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_ca2_alt_fh_020323")
drop_checkpoint(database="dars_nic_391419_j3w9t_collab", temp_df="ccu008_ca2_ggt_fh_020323")
