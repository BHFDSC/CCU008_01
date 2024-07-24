# Databricks notebook source
# MAGIC %run /Repos/frederick.ho@glasgow.ac.uk/CCU008/basic_func

# COMMAND ----------

codelist = spark.table("dss_corporate.gdppr_cluster_refset")

# COMMAND ----------

# DBTITLE 1,show all clusters
#codelist.groupBy("Cluster_ID", 'Cluster_Desc').count().orderBy(F.col("count"), ascending=False).display()

# COMMAND ----------

# DBTITLE 1,check cluster
(codelist
 .filter(F.col("Cluster_ID").isin(["CHOL2_COD"]))
 #.filter(~F.col("ConceptId_Description").contains("Average") & 
 #                  ~F.col("ConceptId_Description").contains("Self measured") & 
 #                  ~F.col("ConceptId_Description").contains("24 hour") & 
 #                  ~F.col("ConceptId_Description").contains("Minimum") & 
 #                  ~F.col("ConceptId_Description").contains("Maximum"))
 #.join(codelist.filter(F.col("Cluster_ID")=="ABPM_COD").select("ConceptId"), ["ConceptId"], how='left_anti')
 .select("ConceptId", "ConceptId_Description")
 .dropDuplicates()
 #.display()
)

# COMMAND ----------

# DBTITLE 1,extract codes
codelist_sel = (
    codelist.filter(F.col("Cluster_ID")=="NDABMI_COD").select("Cluster_ID", "ConceptId", "ConceptId_Description")
            .filter(~F.col("ConceptId_Description").contains("Child"))
    .union(codelist.filter(F.col("Cluster_ID")=="NDASMOK_COD").select("Cluster_ID", "ConceptId", "ConceptId_Description"))
    .union(codelist.filter(F.col("Cluster_ID")=="ALC_COD").select("Cluster_ID", "ConceptId", "ConceptId_Description"))

    .union(codelist.filter(F.col("Cluster_ID")=="NDABP_COD").select("Cluster_ID", "ConceptId", "ConceptId_Description"))
    .union(codelist.filter(F.col("Cluster_ID")=="FASPLASGLUC_COD").select("Cluster_ID", "ConceptId", "ConceptId_Description"))
    .union(codelist.filter(F.col("Cluster_ID")=="IFCCHBAM_COD").select("Cluster_ID", "ConceptId", "ConceptId_Description"))

    .union(codelist.filter(F.col("Cluster_ID")=="CHOL_COD").select("Cluster_ID", "ConceptId", "ConceptId_Description"))
    .union(codelist.filter(F.col("Cluster_ID")=="HDLCCHOL_COD").select("Cluster_ID", "ConceptId", "ConceptId_Description"))
    .union(codelist.filter(F.col("Cluster_ID")=="LDLCCHOL_COD").select("Cluster_ID", "ConceptId", "ConceptId_Description"))
    .union(codelist.filter(F.col("Cluster_ID")=="TRIGLYC_COD").select("Cluster_ID", "ConceptId", "ConceptId_Description"))

    .union(codelist.filter(F.col("Cluster_ID")=="EGFR_COD").select("Cluster_ID", "ConceptId", "ConceptId_Description"))
    .union(codelist.filter(F.col("Cluster_ID")=="LFT_COD").select("Cluster_ID", "ConceptId", "ConceptId_Description"))
)

codelist_sel = (
    codelist_sel.withColumn('rf', 
                            F.when(F.col('Cluster_ID')=='NDABMI_COD', 'BMI')
                             .when(F.col('Cluster_ID')=='NDASMOK_COD', 'Smoking')
                             .when(F.col('Cluster_ID')=='ALC_COD', 'Alcohol')

                             .when(F.col('Cluster_ID')=='NDABP_COD', 'BP')
                             .when(F.col('Cluster_ID')=='FASPLASGLUC_COD', 'Fasting glucose')
                             .when(F.col('Cluster_ID')=='IFCCHBAM_COD', 'HbA1c')

                             .when(F.col('Cluster_ID')=='CHOL_COD', 'Total cholesterol')
                             .when(F.col('Cluster_ID')=='HDLCCHOL_COD', 'HDL cholesterol')
                             .when(F.col('Cluster_ID')=='LDLCCHOL_COD', 'LDL cholesterol')
                             .when(F.col('Cluster_ID')=='TRIGLYC_COD', 'Triglycerides')
                             
                             .when(F.col('Cluster_ID')=='EGFR_COD', 'eGFR')
                             .when(F.col('Cluster_ID')=='LFT_COD', 'LFT'))
)
    

# COMMAND ----------

#codelist_sel.display()

# COMMAND ----------

#codelist_sel.display()
