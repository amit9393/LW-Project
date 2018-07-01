package udlToPDS

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import landingtoudl.ResourceFunctions
import java.util.HashMap

object dimProduct extends ResourceFunctions {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("factSalesForecast").config("spark.executor.cores", 5).config("spark.executor.instances", 18).getOrCreate()
    import spark.implicits._
    
    val DataSet = "dimProduct"
     val adlPath = args(0)
    val ConfigFile = args(1)

    val paramHashMap: HashMap[String, String] = getConfiguration(ConfigFile,DataSet, spark)
    
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(adlPath), hadoopConf)

    /*val TargetDir = "adl://bienodad57179stgadls.azuredatalakestore.net/Unilever/ProductDataStores/LiveWire/India/Dimensions/dimProduct"
    val MRCR_path = "adl://bienodad57179stgadls.azuredatalakestore.net/Unilever/UniversalDataLake/InternalSources/FusionBW/OpenHub/MRCRData/India/YYYY=2016/MM=01/DD=01"
    val Material = "adl://bienodad57179stgadls.azuredatalakestore.net/Unilever/UniversalDataLake/InternalSources/FusionBW/OpenHub/Material/India/IN_MATERIAL.csv"
    val Basepack_CD_SR = "adl://bienodad57179stgadls.azuredatalakestore.net/Unilever/UniversalDataLake/ExternalSources/ManualFiles/OnPremFileShare/BasepackCDDSRMap/India/IN_BASEPACK_CD_DSR_MAP.csv"
    val Basepack_ICD_MAP = "adl://bienodad57179stgadls.azuredatalakestore.net/Unilever/UniversalDataLake/ExternalSources/ManualFiles/OnPremFileShare/BasepackICDMap/India/IN_BASEPACK_ICD_MAP.csv"
    val Basepack_NONCD_FNR = "adl://bienodad57179stgadls.azuredatalakestore.net/Unilever/UniversalDataLake/ExternalSources/ManualFiles/OnPremFileShare/BasepackNonCDFNRMap/India/IN_BASEPACK_NONCD_FNR_MAP.csv"
    val Basepack_WATER_MAP = "adl://bienodad57179stgadls.azuredatalakestore.net/Unilever/UniversalDataLake/ExternalSources/ManualFiles/OnPremFileShare/BasepackWaterMap/India/IN_BASEPACK_WATER_MAP.csv"
    val Material_Type_Text = "adl://bienodad57179stgadls.azuredatalakestore.net/Unilever/UniversalDataLake/InternalSources/FusionBW/OpenHub/MaterialTypeText/India/IN_MATERIAL_TYPE_TEXT.csv"
    val Material_Text = "adl://bienodad57179stgadls.azuredatalakestore.net/Unilever/UniversalDataLake/InternalSources/FusionBW/OpenHub/MaterialText/India/IN_MATTEXT.csv"
    val Material_Group_Text = "adl://bienodad57179stgadls.azuredatalakestore.net/Unilever/UniversalDataLake/InternalSources/FusionBW/OpenHub/MaterialGroupText/India/IN_MATERIALL_GROUP_TEXT.csv"
    val BspkExc_path = "adl://bienodad57179stgadls.azuredatalakestore.net/Unilever/ProductDataStores/LiveWire/India/CategoryDSR/LookupBasepackExclusion"
*/
    
    val TargetDir = adlPath + paramHashMap.getOrDefault("TargetDir", "NA")
    val MRCR_path = adlPath + paramHashMap.getOrDefault("MRCR_path", "NA")
    val Material = adlPath + paramHashMap.getOrDefault("Material", "NA")
    val Basepack_CD_SR = adlPath + paramHashMap.getOrDefault("Basepack_CD_SR", "NA")
    val Basepack_ICD_MAP = adlPath + paramHashMap.getOrDefault("Basepack_ICD_MAP", "NA")
    val Basepack_NONCD_FNR = adlPath + paramHashMap.getOrDefault("Basepack_NONCD_FNR", "NA")
    val Basepack_WATER_MAP = adlPath + paramHashMap.getOrDefault("Basepack_WATER_MAP", "NA")
    val Material_Type_Text = adlPath + paramHashMap.getOrDefault("Material_Type_Text", "NA")
    val Material_Text = adlPath + paramHashMap.getOrDefault("Material_Text", "NA")
    val Material_Group_Text = adlPath + paramHashMap.getOrDefault("Material_Group_Text", "NA")
    val BspkExc_path = adlPath + paramHashMap.getOrDefault("BspkExc_path", "NA")

    val MRCR_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(MRCR_path)
    val Material_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Material)
    val Basepack_CD_SR_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Basepack_CD_SR)
    val Basepack_ICD_MAP_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Basepack_ICD_MAP)
    val Basepack_NONCD_FNR_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Basepack_NONCD_FNR)
    val Basepack_WATER_MAP_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Basepack_WATER_MAP)
    val Material_Type_Text_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Material_Type_Text)
    val Material_Text_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Material_Text)
    val Material_Group_Text_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Material_Group_Text)
    val BspkExc = spark.read.format("com.databricks.spark.csv").option("header", "true").load(BspkExc_path).select("Basepacks Exclusion")

    Material_DF.createOrReplaceTempView("Material_DF_Table")
    Basepack_ICD_MAP_DF.createOrReplaceTempView("Basepack_ICD_MAP_DF_Table")
    Basepack_NONCD_FNR_DF.createOrReplaceTempView("Basepack_NONCD_FNR_DF_Table")
    Basepack_WATER_MAP_DF.createOrReplaceTempView("Basepack_WATER_MAP_DF_Table")
    Basepack_CD_SR_DF.createOrReplaceTempView("Basepack_CD_SR_DF_Table")
    Material_Type_Text_DF.createOrReplaceTempView("Material_Type_Text_Table")
    Material_Text_DF.createOrReplaceTempView("Material_Text_Table")
    Material_Group_Text_DF.createOrReplaceTempView("Material_Group_Text_Table")
    BspkExc.createOrReplaceTempView("BspkExc_Table")
    MRCR_DF.createOrReplaceTempView("MRCR_Table_temp")

    val Selected_Basepack_CD_SR_DF = spark.sql("SELECT BasePack,BasePackDesc,CD_Big_C,CD_Small_C,CD_M_Brand,CD_VARIANTS,CD_SEGMENT,Sales_Category,Sales_Brand,DSR_Internal_Category,DSR_Segment1,DSR_Segment2,DSR_Segment3,DSR_Segment4,DSR_Segment5,DSR_Segment6,DSR_Segment7,MD FROM Basepack_CD_SR_DF_Table")
    val Selected_Basepack_ICD_MAP_DF = spark.sql("SELECT Basepack_Code,Basepack_Desc,Big_C,Category,Brand,Brand_Variant,Format,'' AS Sales_Category,'' AS Sales_Brand,Internal_Category,DSR_Segment_1,DSR_Segment_2,DSR_Segment_3,DSR_Segment_4,DSR_Segment_5,DSR_Segment_6,DSR_Segment_7,MD FROM Basepack_ICD_MAP_DF_Table")
    val Selected_Basepack_NONCD_FNR_DF = spark.sql("SELECT Basepack_Code,Basepack_Desc,Big_C,Category,Brand,Brand_Variant,Format,'' AS Sales_Category,'' AS Sales_Brand,Internal_Category,DSR_Segment_1,DSR_Segment_2,DSR_Segment_3,DSR_Segment_4,DSR_Segment_5,DSR_Segment_6,DSR_Segment_7,MD FROM Basepack_NONCD_FNR_DF_Table")
    val Selected_Basepack_WATER_MAP_DF = spark.sql("SELECT  Basepack,Basepack_Description,Big_C,Small_C,Mother_Brand,Brand_Variant,`Format/_Segment_Sub_Brand`,'' AS Sales_Category,'' AS Sales_Brand,Internal_Category,DSR_Segment_1,DSR_Segment_2,DSR_Segment_3,DSR_Segment_4,DSR_Segment_5,DSR_Segment_6,DSR_Segment_7,MD FROM Basepack_WATER_MAP_DF_Table")

    val Schema_Basepack_mapping = StructType("BasePack,BasePackDesc,CD_Big_C,CD_Small_C,CD_M_Brand,CD_VARIANTS,CD_SEGMENT,Sales_Category,Sales_Brand,DSR_Internal_Category,DSR_Segment1,DSR_Segment2,DSR_Segment3,DSR_Segment4,DSR_Segment5,DSR_Segment6,DSR_Segment7,MD".split(",").map(a => StructField(a, StringType, true)))
    val Sel_Basepack_CD_SR_DF = spark.createDataFrame(Selected_Basepack_CD_SR_DF.rdd, Schema_Basepack_mapping)
    val Sel_Basepack_ICD_MAP_DF = spark.createDataFrame(Selected_Basepack_ICD_MAP_DF.rdd, Schema_Basepack_mapping)
    val Sel_Basepack_NONCD_FNR_DF = spark.createDataFrame(Selected_Basepack_NONCD_FNR_DF.rdd, Schema_Basepack_mapping)
    val Sel_Basepack_WATER_MAP_DF = spark.createDataFrame(Selected_Basepack_WATER_MAP_DF.rdd, Schema_Basepack_mapping)

    val Dis_mrcr_bspk = spark.sql("SELECT a.*,CASE WHEN ROW_NUMBER()OVER(PARTITION BY YBSPACK ORDER BY `0CUST_GROUP__YCARATCUS`)=1 THEN '1' ELSE '0' END AS `Row_Number` FROM MRCR_Table_temp AS a").filter("Row_Number = '1'").drop("Row_Number")

    Dis_mrcr_bspk.createOrReplaceTempView("MRCR_Table")

    val Basepack_Temp_Union = Sel_Basepack_CD_SR_DF.unionAll(Sel_Basepack_ICD_MAP_DF).unionAll(Sel_Basepack_NONCD_FNR_DF).unionAll(Sel_Basepack_WATER_MAP_DF)
    Basepack_Temp_Union.createOrReplaceTempView("Basepack_Union_Temp_Table")

    val Basepack_Union = spark.sql("SELECT *,CASE WHEN ROW_NUMBER() OVER(PARTITION BY BasePack ORDER BY BasePack)=1 THEN '1' ELSE '0' END AS `ROW_NUM` FROM Basepack_Union_Temp_Table").filter("ROW_NUM = '1'").drop("ROW_NUM")

    Basepack_Union.createOrReplaceTempView("Basepack_Union_Table")

    val Material_join_Basepack_Union = spark.sql("""SELECT Material_DF_Table.YMATERL AS PRODUCT_KEY,
Material_DF_Table.YBSPACK AS `BASEPACK_CODE`,
Basepack_Union_Table.BasePackDesc AS BASEPACK,
'' AS CMI_BIGC,
'' AS CMI_SMALLC,
'' AS CMI_CATEGORY,
'' AS CMI_SEGMENT,
'' AS CMI_BRAND,
'' AS CMI_BRANDVARIANT,
Basepack_Union_Table.CD_Big_C AS CD_BIGC,
Basepack_Union_Table.CD_Small_C AS CD_SMALLC,
Basepack_Union_Table.CD_M_Brand AS CD_MBRAND,
 Basepack_Union_Table.CD_VARIANTS AS CD_VARIANTS, 
Basepack_Union_Table.CD_SEGMENT AS CD_SEGMENT,
'' AS FORMAT,
 Material_DF_Table.YSTDBUSG AS SALES_BUSINESSGROUP,
 Material_DF_Table.YSTDBUS AS SALES_BUSINESS,
IF(Basepack_Union_Table.Sales_Category = '',Material_DF_Table.YSLSCATY,Basepack_Union_Table.Sales_Category) AS SALES_CATEGORY,
 Material_DF_Table.YSSUSCTY AS SALES_SUBCATEGORY,
 IF(Basepack_Union_Table.Sales_Brand = '',Material_DF_Table.YSLSBRND,Basepack_Union_Table.Sales_Brand) AS SALES_BRAND, 
Material_DF_Table.YBRNDVR AS SALES_BRANDVARIANT,
 Material_DF_Table.YBUSNSS AS BUSINESS,
Material_DF_Table.YBUSGRP AS BUSINESS_GROUP,
Material_DF_Table.YPROFCTR AS PROFIT_CENTRE,
Material_DF_Table.YSTDSCAT AS CATEGORY,
'' AS SUB_CATEGORY,
Material_DF_Table.YSTDBRND AS BRAND,
Material_DF_Table.YSTDBRVR AS BRAND_VARIANT,
 Material_DF_Table.DIVISION AS DIVISION,
 '' AS MARKET,
 Material_DF_Table.IND_SECTOR AS SECTOR,
Basepack_Union_Table.DSR_Internal_Category AS INTERNAL_CATEGORY,
Basepack_Union_Table.DSR_Segment1 AS DSR_SEGMENT1,
Basepack_Union_Table.DSR_Segment2 AS DSR_Segment2,
Basepack_Union_Table.DSR_Segment3 AS DSR_Segment3,
Basepack_Union_Table.DSR_Segment4 AS DSR_Segment4,
Basepack_Union_Table.DSR_Segment5 AS DSR_Segment5,
Basepack_Union_Table.DSR_Segment6 AS DSR_Segment6,
Basepack_Union_Table.DSR_Segment7 AS DSR_Segment7,
Basepack_Union_Table.MD AS MD, 
'' AS FMCG,
 Material_DF_Table.MATL_TYPE AS MATL_TYPE,
Material_DF_Table.MATL_GROUP AS MATL_GROUP FROM Material_DF_Table 
LEFT OUTER JOIN Basepack_Union_Table ON Material_DF_Table.YBSPACK = Basepack_Union_Table.BasePack""")
    Material_join_Basepack_Union.createOrReplaceTempView("Material_join_Basepack_Union_Table")

    val Main_join_Material_Type_Text = spark.sql("SELECT Material_join_Basepack_Union_Table.*,Material_Type_Text_Table.TXTMD AS MATERIAL_TYPE FROM Material_join_Basepack_Union_Table LEFT OUTER JOIN Material_Type_Text_Table ON Material_join_Basepack_Union_Table.MATL_TYPE = Material_Type_Text_Table.MATL_TYPE")
    Main_join_Material_Type_Text.createOrReplaceTempView("Main_join_Material_Type_Text_Table")

    val Main_join_Material_Text = spark.sql("SELECT Main_join_Material_Type_Text_Table.*,Material_Text_Table.TXTLG AS MATERIAL_NAME FROM Main_join_Material_Type_Text_Table LEFT OUTER JOIN Material_Text_Table ON Main_join_Material_Type_Text_Table.PRODUCT_KEY = Material_Text_Table.YMATERL")
    Main_join_Material_Text.createOrReplaceTempView("Main_join_Material_Text_Table")

    val Main_join_Material_Group_Text = spark.sql("SELECT Main_join_Material_Text_Table.*,Material_Group_Text_Table.SHORT_DESCRIPTION AS MATERIAL_GROUP FROM Main_join_Material_Text_Table LEFT OUTER JOIN Material_Group_Text_Table ON Main_join_Material_Text_Table.MATL_GROUP = Material_Group_Text_Table.SHORT_DESCRIPTION")
    Main_join_Material_Group_Text.createOrReplaceTempView("Main_join_Material_Group_Text_Table")

    val Ybs_N_Mat = spark.sql("SELECT YBSPACK FROM MRCR_Table WHERE NOT EXISTS (SELECT YBSPACK FROM Material_DF_Table WHERE MRCR_Table.YBSPACK = Material_DF_Table.YBSPACK)").filter("YBSPACK is not null").distinct

    Ybs_N_Mat.createOrReplaceTempView("Ybs_N_Mat_Table")

    val Res_Ybs_N_Mat = spark.sql("SELECT 'Non_existent Base pack' AS `PRODUCT_KEY`,'' AS `BASEPACK_CODE`,`YBSPACK` AS `BASEPACK`,'' AS `CMI_BIGC`,'' AS `CMI_SMALLC`,'' AS `CMI_CATEGORY`,'' AS `CMI_SEGMENT`,'' AS `CMI_BRAND`,'' AS `CMI_BRANDVARIANT`,'' AS `CD_BIGC`,'' AS `CD_SMALLC`,'' AS `CD_MBRAND`,'' AS `CD_VARIANTS`,'' AS `CD_SEGMENT`,'' AS `FORMAT`,'' AS `SALES_BUSINESSGROUP`,'' AS `SALES_BUSINESS`,'' AS `SALES_CATEGORY`,'' AS `SALES_SUBCATEGORY`,'' AS `SALES_BRAND`,'' AS `SALES_BRANDVARIANT`,'' AS `BUSINESS`,'' AS `BUSINESS_GROUP`,'' AS `PROFIT_CENTRE`,'' AS `CATEGORY`,'' AS `SUB_CATEGORY`,'' AS `BRAND`,'' AS `BRAND_VARIANT`,'' AS `DIVISION`,'' AS `MARKET`,'' AS `SECTOR`,'' AS `INTERNAL_CATEGORY`,'' AS `DSR_SEGMENT1`,'' AS `DSR_Segment2`,'' AS `DSR_Segment3`,'' AS `DSR_Segment4`,'' AS `DSR_Segment5`,'' AS `DSR_Segment6`,'' AS `DSR_Segment7`,'' AS `MD`,'' AS `FMCG`,'' AS `MATL_TYPE`,'' AS `MATL_GROUP`,'' AS `MATERIAL_TYPE`,'' AS `MATERIAL_NAME`,'' AS `MATERIAL_GROUP` FROM Ybs_N_Mat_Table")

    val ResMain = Res_Ybs_N_Mat.unionAll(Main_join_Material_Group_Text)

    ResMain.createOrReplaceTempView("ResMain_Table")

    val BskExcluded = spark.sql("SELECT a.*, IF(bsk.`Basepacks Exclusion` is not null, 'Exclude',null) AS BASEPACK_CLASS FROM ResMain_Table AS a LEFT OUTER JOIN BspkExc_Table AS bsk ON bsk.`Basepacks Exclusion` = a.`BASEPACK_CODE`")

    val bsp_cls_null = BskExcluded.filter("BASEPACK_CLASS is null")

    bsp_cls_null.createOrReplaceTempView("bsp_cls_null_table")

    val bsp_cls_not_null = BskExcluded.filter("BASEPACK_CLASS is not null")

    bsp_cls_not_null.createOrReplaceTempView("bsp_cls_not_null_table")

    val Mrcr_join_bps_cls_null = spark.sql("SELECT bsp.*, mrcr.`0RECTYPE`, mrcr.`0DISTR_CHAN`, mrcr.`0COMP_CODE` FROM bsp_cls_null_table as bsp LEFT JOIN MRCR_Table AS mrcr ON mrcr.YBSPACK = bsp.BASEPACK_CODE")

    Mrcr_join_bps_cls_null.createOrReplaceTempView("Mrcr_join_bps_cls_null_table")

    val NonCD_union = Sel_Basepack_ICD_MAP_DF.union(Sel_Basepack_NONCD_FNR_DF).union(Sel_Basepack_WATER_MAP_DF).select("BasePack").distinct

    NonCD_union.createOrReplaceTempView("NonCD_union_table")

    val CD_NonCD = spark.sql("SELECT bmr.*, nun.BasePack AS `Union_Bspk` FROM Mrcr_join_bps_cls_null_table AS bmr LEFT OUTER JOIN NonCD_union_table AS nun ON nun.BasePack = bmr.BASEPACK_CODE")

    CD_NonCD.createOrReplaceTempView("CD_NonCD_table")

    val ResFinalOp = spark.sql("SELECT *,CASE WHEN Union_Bspk is not null AND 0RECTYPE IN ('F','B','D') AND 0DISTR_CHAN != 'ST' THEN 'NONCD' WHEN 0RECTYPE = 'F' AND substring(0COMP_CODE,1,1) = 'H' THEN 'CD' ELSE '' END AS Derived_BasepackClass FROM CD_NonCD_table").drop("BASEPACK_CLASS").withColumnRenamed("Derived_BasepackClass", "BASEPACK_CLASS")

    ResFinalOp.createOrReplaceTempView("ResFinalOp_table")

    val Res_op_BasepackClass = spark.sql("SELECT PRODUCT_KEY,BASEPACK_CODE,BASEPACK,CMI_BIGC,CMI_SMALLC,CMI_CATEGORY,CMI_SEGMENT,CMI_BRAND,CMI_BRANDVARIANT,CD_BIGC,CD_SMALLC,CD_MBRAND,CD_VARIANTS,CD_SEGMENT,FORMAT,SALES_BUSINESSGROUP,SALES_BUSINESS,SALES_CATEGORY,SALES_SUBCATEGORY,SALES_BRAND,SALES_BRANDVARIANT,BUSINESS,BUSINESS_GROUP,PROFIT_CENTRE,CATEGORY,SUB_CATEGORY,BRAND,BRAND_VARIANT,DIVISION,MARKET,SECTOR,INTERNAL_CATEGORY,DSR_SEGMENT1,DSR_Segment2,DSR_Segment3,DSR_Segment4,DSR_Segment5,DSR_Segment6,DSR_Segment7,MD,FMCG,MATL_TYPE,MATL_GROUP,MATERIAL_TYPE,MATERIAL_NAME,MATERIAL_GROUP,BASEPACK_CLASS FROM ResFinalOp_table").unionAll(bsp_cls_not_null)

    Res_op_BasepackClass.createOrReplaceTempView("Res_op_BasepackClass_table")

    val CurrentDate_1 = CurrentDate()

    val Res_CrtDtDF = spark.sql("SELECT *,'' AS DATASOURCE_NAME,'SPARK_ADMIN' AS CREATED_BY,'" + CurrentDate_1 + "' AS CREATED_DATE,'SPARK_ADMIN' AS MODIFIED_BY,'" + CurrentDate_1 + "' AS MODIFIED_DATE FROM Res_op_BasepackClass_table")

    Res_CrtDtDF.createOrReplaceTempView("Res_CrtDtDF_Table")

    val FirstDF_Temp = spark.sql("SELECT * , CASE WHEN ROW_NUMBER()OVER(PARTITION BY BASEPACK_CODE ORDER BY PRODUCT_KEY)=1 THEN '1' ELSE '0' END AS PRIMARY_MATERIAL FROM Res_CrtDtDF_Table")

    FirstDF_Temp.createOrReplaceTempView("FirstDF_Temp_table")

    val FirstDF = spark.sql("SELECT PRODUCT_KEY,BASEPACK_CODE,BASEPACK,CMI_BIGC,CMI_SMALLC,CMI_CATEGORY,CMI_SEGMENT,CMI_BRAND,CMI_BRANDVARIANT,CD_BIGC,CD_SMALLC,CD_MBRAND,CD_VARIANTS,CD_SEGMENT,FORMAT,PRIMARY_MATERIAL,SALES_BUSINESSGROUP,SALES_BUSINESS,SALES_CATEGORY,SALES_SUBCATEGORY,SALES_BRAND,SALES_BRANDVARIANT,BUSINESS,BUSINESS_GROUP,PROFIT_CENTRE,CATEGORY,SUB_CATEGORY,BRAND,BRAND_VARIANT,DIVISION,MARKET,SECTOR,INTERNAL_CATEGORY,DSR_SEGMENT1,DSR_SEGMENT2,DSR_SEGMENT3,DSR_SEGMENT4,DSR_SEGMENT5,DSR_SEGMENT6,DSR_SEGMENT7,MD,FMCG,BASEPACK_CLASS,MATERIAL_TYPE,MATERIAL_NAME,MATERIAL_GROUP,DATASOURCE_NAME,CREATED_BY,CREATED_DATE,MODIFIED_BY,MODIFIED_DATE FROM FirstDF_Temp_table")

    FirstDF.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(TargetDir)
  }
}