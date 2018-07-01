package udlToPDS

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import landingtoudl.ResourceFunctions
import java.util.HashMap
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.fs.Path

object factPrimarySalesFinance extends ResourceFunctions{
  def main(args: Array[String]): Unit = {

    //val spark = SparkSession.builder().appName("factSalesForecast").config("spark.executor.cores",5).config("spark.executor.instances", 40).getOrCreate()
    val spark = SparkSession.builder().appName("factSalesForecast").config("spark.dynamicAllocation.enabled","true").config("spark.shuffle.service.enabled", "true").getOrCreate()
    import spark.implicits._
    
    
    val DataSet = "factPrimarySalesFinance"
     val adlPath = args(0)
    val ConfigFile = args(1)

    val paramHashMap: HashMap[String, String] = getConfiguration(ConfigFile,DataSet, spark)
    
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(adlPath), hadoopConf)
  

    val MRCR_path = adlPath + paramHashMap.getOrDefault("MRCR_path", "NA")
    val dimTime_Path = adlPath + paramHashMap.getOrDefault("dimTime_Path", "NA")
    val dimProduct_Path = adlPath + paramHashMap.getOrDefault("dimProduct_Path", "NA")
    val dimCustomer_Path = adlPath + paramHashMap.getOrDefault("dimCustomer_Path", "NA")
    val dimCountry_Path = adlPath + paramHashMap.getOrDefault("dimCountry_Path", "NA")
    val dimCurrency_Path = adlPath + paramHashMap.getOrDefault("dimCurrency_Path", "NA")
    val exclusionBspk = adlPath + paramHashMap.getOrDefault("exclusionBspk", "NA")
    val CompanyCode = adlPath + paramHashMap.getOrDefault("CompanyCode", "NA")
    val dimTimePeriod = adlPath + paramHashMap.getOrDefault("dimTimePeriod", "NA")
    val BillTypInc_path = adlPath + paramHashMap.getOrDefault("BillTypInc_path", "NA")
    val Destination = adlPath + paramHashMap.getOrDefault("Destination", "NA")
    val AggCol = paramHashMap.getOrDefault("AggCol", "NA").split('|')
    val CombinationColumn_Days = paramHashMap.getOrDefault("CombinationColumn", "NA").split(",")
    val CombinationColumn_Other = paramHashMap.getOrDefault("CombinationColumn_Other", "NA").split(",")
    val DateColumn_Tup = (paramHashMap.getOrDefault("DateColumn", "NA"),paramHashMap.getOrDefault("DateColumn_format", "NA"))
    
    if(!hdfs.isDirectory(new org.apache.hadoop.fs.Path(Destination)))
    {
      hdfs.mkdirs(new org.apache.hadoop.fs.Path(Destination))
    }
    
    
    val MRCR_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(MRCR_path)
    val dimTimeDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimTime_Path)
    val dimProductDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimProduct_Path)
    val dimCustomerDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimCustomer_Path)
    val dimCountryDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimCountry_Path)
    val dimCurrencyDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimCurrency_Path)
    val bspkExclusionDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(exclusionBspk)
    val CompCodeExe = spark.read.format("com.databricks.spark.csv").option("header", "true").load(CompanyCode)
    val dimTimePeriod_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimTimePeriod)
    val BillTypInc = spark.read.format("com.databricks.spark.csv").option("header", "true").load(BillTypInc_path)

    MRCR_DF.createOrReplaceTempView("MRCR_Table")
    dimTimeDF.createOrReplaceTempView("dimTime_Table")
    dimProductDF.createOrReplaceTempView("dim_Product_Table")
    dimCustomerDF.createOrReplaceTempView("dim_Customer_Table")
    dimCountryDF.createOrReplaceTempView("dim_Country_Table")
    dimCurrencyDF.createOrReplaceTempView("dimCurrency_Table")
    bspkExclusionDF.createOrReplaceTempView("bspkExclusion_Table")
    CompCodeExe.createOrReplaceTempView("CompanyCodeExe_Table")
    dimTimePeriod_DF.createOrReplaceTempView("dimTimePeriod_Table")
    BillTypInc.createOrReplaceTempView("BillingTypeInclusion_Table")

    val ResDF_temp = spark.sql("""SELECT f.`0BILL_TYPE` AS `BILLING_TYPE`,
f.`0BUS_AREA` AS `BUSINESS_AREA`,
f.`0CUST_GROUP__YCARATCUS` AS `CPG`,
f.`0PROFIT_CTR__YCARATENT` AS `SECTOR`,
f.`0CHRT_ACCTS` AS `CHART_OF_ACCOUNTS`,
f.`0COMP_CODE` AS `COMPANY_CODE`,
f.`0CO_AREA` AS `CONTROLLING_AREA_SATMPED`,
f.`0COSTCENTER` AS `COST_CENTER`,
f.`0COSTELMNT` AS `COST_ELEMENT`,
f.`0RECTYPE` AS `RECORD_TYPE`,
f.`YSHREG` AS `REGION_SHIP_TO`,
f.`0REC_TYPE` AS `TRANSACTION_TYPE`,
f.`0G_UVV051` AS `UN_CIGS_VOL`,
f.`0G_UVV050` AS `UN_CIGS_VOL_TON`,
f.`0G_UVV052` AS `UN_CAPTIVE_VOLU`,
f.`0G_UVV001` AS `UN_NET_WT`,
f.`0G_QABSMG` AS `UN_SALES_QUANTI`,
f.`0VERSION` AS `VERSION`,
f.`0G_AVV272` AS `CIGS_TURNO`,
f.`0G_AVV017` AS `CIGS_UNITS`,
f.`0G_QVV051` AS `CIGS_VOL_UNITS`,
(f.`0G_QVV050` + f.`0G_QVV051`) AS `CIGS_VOLUME`,
f.`0G_QVV052` AS `CAPTIVE_VOLUME`,
f.`0G_AVV110` AS `GSV_DIST`,
(f.`0G_AVV100` - f.`0G_AVV110` - f.`0G_AVV022` + f.`0G_AVV017`) AS `GSV_3P`,
(f.`0G_AVV100` - f.`0G_AVV110` - f.`0G_AVV022` + f.`0G_AVV017` + f.`0G_AVV272`) AS `GROSS_GSV`,
f.`0G_AVV022` AS `MEMO_DAM_DISC`,
f.`0G_QVV001` AS `VOL_W_MEMO_WO_POGO`,
(f.`0G_AVV100`-f.`0G_AVV110`-f.`0G_AVV022`-f.`0G_AVV017`+f.`0G_AVV272`) AS `GSV_W_MEMO_WO_POGO`,
CASE
WHEN f.`0BILL_TYPE` NOT IN ('ZS03','ZR03','ZS34','ZSM') THEN f.`0G_QVV001`
ELSE '0'
END AS `VOL_WO_MEMO_WO_POGO`,
CASE
WHEN f.`0BILL_TYPE` NOT IN ('ZS03','ZR03','ZS34','ZSM') THEN (f.`0G_AVV100`-f.`0G_AVV110`-f.`0G_AVV022`-f.`0G_AVV017`+f.`0G_AVV272`)
ELSE '0'
END AS `GSV_WO_MEMO_WO_POGO`,
f.`0G_QABSMG` AS `VOL_EACHES_W_MEMO_WO_POGO`,
CASE
WHEN `0BILL_TYPE` NOT IN ('ZS03','ZR03','ZS34','ZSM') THEN f.`0G_QABSMG`
ELSE '0'
END AS `VOL_EACHES_WO_MEMO_WO_POGO`,
CASE
WHEN f.`0COSTELMNT`= '0002120362' THEN f.`0G_AVV107`
WHEN f.`0COSTELMNT`= '#' THEN f.`0G_AVV245`
WHEN f.`0COSTELMNT` IN ('0002120128','0002120206','0002120901') THEN f.`0G_AVV246`
WHEN f.`0COSTELMNT`= '0002123024' THEN f.`0G_AVV485`
ELSE NVL(f.`0COSTELMNT`,0)
END AS `POGO`,
CASE
WHEN f.`0BILL_TYPE` NOT IN ('ZS03','ZR03','ZS34','ZSM') THEN 'Y'
ELSE 'N'
END AS `MEMO`,
dt.`TIME_KEY` AS `TIME_KEY`,
lpad(f.`YMATERL`,18,'0') As `PRODUCT_KEY`,
lpad(f.`YCUSTMER`,10,'0') AS `CUSTOMER_KEY`,
dcnt.`COUNTRY_KEY` AS `COUNTRY_KEY`,
dcur.`CURRENCY_KEY` AS `CURRENCY_KEY`,
CASE 
WHEN dpt.`BASEPACK_CLASS` = 'CD' AND BTI.`Billing Types Inclusion` IS NOT NULL THEN 
(CASE
WHEN f.`0BILL_TYPE` NOT IN ('ZS03','ZR03','ZS34','ZSM') THEN (f.`0G_AVV100`-f.`0G_AVV110`-f.`0G_AVV022`-f.`0G_AVV017`+f.`0G_AVV272`)
ELSE '0'
END) + (CASE
WHEN f.`0COSTELMNT`= '0002120362' THEN f.`0G_AVV107`
WHEN f.`0COSTELMNT`= '#' THEN f.`0G_AVV245`
WHEN f.`0COSTELMNT` IN ('0002120128','0002120206','0002120901') THEN f.`0G_AVV246`
WHEN f.`0COSTELMNT`= '0002123024' THEN f.`0G_AVV485`
ELSE NVL(f.`0COSTELMNT`,0)
END)
ELSE '0'
END AS `CD_TOTAL_GSV`,
CASE 
WHEN dpt.`BASEPACK_CLASS` = 'CD' AND BTI.`Billing Types Inclusion` IS NOT NULL THEN (f.`0G_AVV100`-f.`0G_AVV110`-f.`0G_AVV022`-f.`0G_AVV017`+f.`0G_AVV272`) + (CASE
WHEN f.`0COSTELMNT`= '0002120362' THEN f.`0G_AVV107`
WHEN f.`0COSTELMNT`= '#' THEN f.`0G_AVV245`
WHEN f.`0COSTELMNT` IN ('0002120128','0002120206','0002120901') THEN f.`0G_AVV246`
WHEN f.`0COSTELMNT`= '0002123024' THEN f.`0G_AVV485`
ELSE NVL(f.`0COSTELMNT`,0)
END)
ELSE '0'
END AS `MEMO_CD_GSV`,
CASE 
WHEN dpt.`BASEPACK_CLASS` = 'NONCD' AND cce.`Companycodes Exclusion` IS NOT NULL THEN (f.`0G_AVV100`-f.`0G_AVV110`-f.`0G_AVV022`-f.`0G_AVV017`+f.`0G_AVV272`) + (CASE
WHEN f.`0COSTELMNT`= '0002120362' THEN f.`0G_AVV107`
WHEN f.`0COSTELMNT`= '#' THEN f.`0G_AVV245`
WHEN f.`0COSTELMNT` IN ('0002120128','0002120206','0002120901') THEN f.`0G_AVV246`
WHEN f.`0COSTELMNT`= '0002123024' THEN f.`0G_AVV485`
ELSE NVL(f.`0COSTELMNT`,0)
END)
ELSE '0'
END AS `NONCD_TOTAL_GSV`,
CASE
WHEN bxe.`Basepacks Exclusion` IS NOT NULL THEN (CASE
WHEN f.`0BILL_TYPE` NOT IN ('ZS03','ZR03','ZS34','ZSM') THEN (f.`0G_AVV100`-f.`0G_AVV110`-f.`0G_AVV022`-f.`0G_AVV017`+f.`0G_AVV272`)
ELSE '0'
END) + (CASE
WHEN f.`0COSTELMNT`= '0002120362' THEN f.`0G_AVV107`
WHEN f.`0COSTELMNT`= '#' THEN f.`0G_AVV245`
WHEN f.`0COSTELMNT` IN ('0002120128','0002120206','0002120901') THEN f.`0G_AVV246`
WHEN f.`0COSTELMNT`= '0002123024' THEN f.`0G_AVV485`
ELSE NVL(f.`0COSTELMNT`,0)
END)
ELSE '0'
END AS `EXCLUDED_BP_CD_GSV`,
CASE
WHEN bxe.`Basepacks Exclusion` IS NOT NULL AND cce.`Companycodes Exclusion` IS NULL AND f.`0RECTYPE` IN ('B','D') AND f.`0DISTR_CHAN` != 'ST' THEN
(CASE
WHEN f.`0BILL_TYPE` NOT IN ('ZS03','ZR03','ZS34','ZSM') THEN (f.`0G_AVV100`-f.`0G_AVV110`-f.`0G_AVV022`-f.`0G_AVV017`+f.`0G_AVV272`)
ELSE '0'
END) + (CASE
WHEN f.`0COSTELMNT`= '0002120362' THEN f.`0G_AVV107`
WHEN f.`0COSTELMNT`= '#' THEN f.`0G_AVV245`
WHEN f.`0COSTELMNT` IN ('0002120128','0002120206','0002120901') THEN f.`0G_AVV246`
WHEN f.`0COSTELMNT`= '0002123024' THEN f.`0G_AVV485`
ELSE NVL(f.`0COSTELMNT`,0)
END)
ELSE '0'
END AS `FIN_ITEM_GSV`,
CASE
WHEN cce.`Companycodes Exclusion` IS NULL AND f.`0DISTR_CHAN` IN ('ST','OH') THEN
(CASE
WHEN f.`0BILL_TYPE` NOT IN ('ZS03','ZR03','ZS34','ZSM') THEN (f.`0G_AVV100`-f.`0G_AVV110`-f.`0G_AVV022`-f.`0G_AVV017`+f.`0G_AVV272`)
ELSE '0'
END) + (CASE
WHEN f.`0COSTELMNT`= '0002120362' THEN f.`0G_AVV107`
WHEN f.`0COSTELMNT`= '#' THEN f.`0G_AVV245`
WHEN f.`0COSTELMNT` IN ('0002120128','0002120206','0002120901') THEN f.`0G_AVV246`
WHEN f.`0COSTELMNT`= '0002123024' THEN f.`0G_AVV485`
ELSE NVL(f.`0COSTELMNT`,0)
END)
ELSE '0'
END AS `ST_OH`,
CASE
WHEN bxe.`Basepacks Exclusion` IS NOT NULL AND cce.`Companycodes Exclusion` IS NULL AND f.`0RECTYPE` = 'F' AND f.`0DISTR_CHAN` != 'ST' THEN
(CASE
WHEN f.`0BILL_TYPE` NOT IN ('ZS03','ZR03','ZS34','ZSM') THEN (f.`0G_AVV100`-f.`0G_AVV110`-f.`0G_AVV022`-f.`0G_AVV017`+f.`0G_AVV272`)
ELSE '0'
END) + (CASE
WHEN f.`0COSTELMNT`= '0002120362' THEN f.`0G_AVV107`
WHEN f.`0COSTELMNT`= '#' THEN f.`0G_AVV245`
WHEN f.`0COSTELMNT` IN ('0002120128','0002120206','0002120901') THEN f.`0G_AVV246`
WHEN f.`0COSTELMNT`= '0002123024' THEN f.`0G_AVV485`
ELSE NVL(f.`0COSTELMNT`,0)
END)
ELSE '0'
END AS `NON_H_COMP_CODE`,
'' AS `UOM_KEY`,
CASE 
WHEN dpt.`BASEPACK_CLASS` ='NONCD' THEN (CASE
WHEN f.`0COSTELMNT`= '0002120362' THEN f.`0G_AVV107`
WHEN f.`0COSTELMNT`= '#' THEN f.`0G_AVV245`
WHEN f.`0COSTELMNT` IN ('0002120128','0002120206','0002120901') THEN f.`0G_AVV246`
WHEN f.`0COSTELMNT`= '0002123024' THEN f.`0G_AVV485`
ELSE NVL(f.`0COSTELMNT`,0)
END)
ELSE '0'
END AS `INV_BILL_TYPE`
FROM MRCR_Table  AS f
LEFT OUTER JOIN dimTime_Table AS dt ON  dt.`TIME_KEY` = f.`0PSTNG_DATE`
LEFT OUTER JOIN dim_Product_Table as dpt ON dpt.`PRODUCT_KEY` = lpad(f.`YMATERL`,18,'0')
LEFT OUTER JOIN dim_Customer_Table as dct ON  dct.`CUSTOMER_KEY` = lpad(f.`YCUSTMER`,10,'0')
LEFT OUTER JOIN dim_Country_Table as dcnt ON dcnt.`COUNTRY_NAME` = f.`0COUNTRY`
LEFT OUTER JOIN dimCurrency_Table AS dcur ON  dcur.`CURRENCY_NAME` = f.`0CURRENCY`
LEFT OUTER JOIN bspkExclusion_Table AS bxe ON  bxe.`Basepacks Exclusion` = f.`YBSPACK`
LEFT OUTER JOIN CompanyCodeExe_Table AS cce ON  cce.`Companycodes Exclusion` = f.`0COMP_CODE`
LEFT OUTER JOIN BillingTypeInclusion_Table AS BTI ON BTI.`Billing Types Inclusion` = f.`0BILL_TYPE`
""").as("mainDF").select($"mainDF.*",
      ($"GSV_WO_MEMO_WO_POGO" + $"POGO").as("GSV_WO_MEMO_W_POGO"),
      ($"VOL_WO_MEMO_WO_POGO" + $"POGO").as("VOL_WO_MEMO_W_POGO"),
      ($"VOL_EACHES_WO_MEMO_WO_POGO" + $"POGO").as("VOL_EACHES_WO_MEMO_W_POGO"))

    ResDF_temp.createOrReplaceTempView("ResDF_temp_Table")

    val ResDF = spark.sql("SELECT *, CONCAT(`PRODUCT_KEY`,'_',`CUSTOMER_KEY`) AS `DIM_COMBINATION` FROM ResDF_temp_Table")

    ResDF.cache()
    
    //ResDF.repartition(1).write.format("com.databricks.spark.csv").option("header","true").mode("overwrite").save("adl://bienodad57179stgadls.azuredatalakestore.net/Unilever/Testing/factPrimarySalesFinance")
    
    ResDF.createOrReplaceTempView("FactTable")
    dimTimeDF.createOrReplaceTempView("Date_Table")

//timeSpan_process(AggCol, CombinationColumn, DateColumn_Tup, spark)
    timeSpan_process_temp(AggCol, CombinationColumn_Days,CombinationColumn_Other, DateColumn_Tup,("TIME_KEY",paramHashMap.getOrDefault("DateColumn_format", "NA")), spark)
    
    
//timeSpan_process(AggCol, CombinationColumn, DateColumn_Tup, spark)

/*val CurrentDt = CurrentDate()
val ResOp = spark.sql("SELECT * FROM op_table").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumnRenamed("CREATED_DT", "CREATED_DATE")

ResOp.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination)
*/

val CurrentDt = CurrentDate()
if(!hdfs.isDirectory(new Path(Destination+"/Days")))
{
  hdfs.mkdirs(new Path(Destination+"/Days"))
}
spark.sql("SELECT * FROM res_factDF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Days")


if(!hdfs.isDirectory(new Path(Destination+"/Agg/MTD")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/MTD"))
  
}
spark.sql("SELECT * FROM res_MTD_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/MTD")

if(!hdfs.isDirectory(new Path(Destination+"/Agg/Months")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/Months"))
  
}
spark.sql("SELECT * FROM res_Month_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/Months")

if(!hdfs.isDirectory(new Path(Destination+"/Agg/YTD")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/YTD"))
  
}
spark.sql("SELECT * FROM res_YTD_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/YTD")

if(!hdfs.isDirectory(new Path(Destination+"/Agg/QTD")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/QTD"))
  
}
spark.sql("SELECT * FROM res_QTD_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/QTD")

if(!hdfs.isDirectory(new Path(Destination+"/Agg/Year")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/Year"))
  
}
spark.sql("SELECT * FROM res_Year_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/Year")

if(!hdfs.isDirectory(new Path(Destination+"/Agg/Quarter")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/Quarter"))
  
}
spark.sql("SELECT * FROM res_Quarter_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/Quarter")

if(!hdfs.isDirectory(new Path(Destination+"/Agg/L3M")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/L3M"))
  
}
spark.sql("SELECT * FROM res_L3M_DF").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/L3M")

if(!hdfs.isDirectory(new Path(Destination+"/Agg/MAT")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/MAT"))
  
}
spark.sql("SELECT * FROM res_MAT_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/MAT")

if(!hdfs.isDirectory(new Path(Destination+"/Agg/MOC")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/MOC"))
  
}
spark.sql("SELECT * FROM res_MOC_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/MOC")

if(!hdfs.isDirectory(new Path(Destination+"/Agg/TDP")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/TDP"))
  
}
spark.sql("SELECT * FROM res_TDP_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/TDP")

if(!hdfs.isDirectory(new Path(Destination+"/Agg/L30D")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/L30D"))
  
}
spark.sql("SELECT * FROM res_LTD_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/L30D")

  }
}