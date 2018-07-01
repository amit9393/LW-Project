package udlToPDS

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import landingtoudl.ResourceFunctions
import java.util.HashMap
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.fs.Path

object factSalesForecast extends ResourceFunctions{
  def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder().appName("factSalesForecast").config("spark.executor.cores",5).config("spark.executor.instances", 18).getOrCreate()
    import spark.implicits._
    
       
    
     val DataSet = "factSalesForecast"
     val adlPath = args(0)
    val ConfigFile = args(1)
    
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(adlPath), hadoopConf)
  
    

    val paramHashMap: HashMap[String, String] = getConfiguration(ConfigFile,DataSet, spark)

    val FInantial_Forecast_path = adlPath + paramHashMap.getOrDefault("FInantial_Forecast_path", "NA")

    val dimProduct_path = adlPath + paramHashMap.getOrDefault("dimProduct_path", "NA")

    val dimCurrency_path = adlPath + paramHashMap.getOrDefault("dimCurrency_path", "NA")

    val dimCustomer_path = adlPath + paramHashMap.getOrDefault("dimCustomer_path", "NA")

    val dimCountry_path = adlPath + paramHashMap.getOrDefault("dimCountry_path", "NA")
    
    val dimTime_path = adlPath + paramHashMap.getOrDefault("dimTime_path", "NA")
    
    val dimTimePeriod_path = adlPath + paramHashMap.getOrDefault("dimTimePeriod_path", "NA")
    
    val Destination = adlPath + paramHashMap.getOrDefault("Destination", "NA")
    
    val AggCol = paramHashMap.getOrDefault("AggCol", "NA").split('|')
    val CombinationColumn_Days = paramHashMap.getOrDefault("CombinationColumn", "NA").split(",")
    val CombinationColumn_Other = paramHashMap.getOrDefault("CombinationColumn_Other", "NA").split(",")
    val DateColumn_Tup = (paramHashMap.getOrDefault("DateColumn", "NA"),paramHashMap.getOrDefault("DateColumn_format", "NA"))
    
     if(!hdfs.isDirectory(new org.apache.hadoop.fs.Path(Destination)))
    {
      hdfs.mkdirs(new org.apache.hadoop.fs.Path(Destination))
    }
   
    val FInantial_Forecast_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(FInantial_Forecast_path)


    val dimProduct_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimProduct_path).where("PRIMARY_MATERIAL='1'")

    val dimCurrency_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimCurrency_path)

    val dimCustomer_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimCustomer_path)

    val dimCountry_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimCountry_path)
    
    val dimTime_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimTime_path)
    
    val dimTimePeriod_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimTimePeriod_path)

    FInantial_Forecast_DF.createOrReplaceTempView("FInantial_Forecast_Table")

    dimProduct_DF.createOrReplaceTempView("dimProduct_Table")

    dimCurrency_DF.createOrReplaceTempView("dimCurrency_Table")

    dimCustomer_DF.createOrReplaceTempView("dimCustomer_Table")

    dimCountry_DF.createOrReplaceTempView("dimCountry_Table")
    
    dimTime_DF.createOrReplaceTempView("dimTime_Table")
    
    dimTimePeriod_DF.createOrReplaceTempView("dimTimePeriod_Table")

    val Selected_DF = spark.sql("""SELECT 
                                 FISCYEAR,
                                `BUS_AREA` AS `BUSINESS_AREA`,
                                `/BIC/YGLHYEAR` AS `GL_HALFYR`,
                                `/BIC/YGLQTR` AS `GL_YR_QTR`,
                                `UNIT` AS `UNIT_OF_MEASURE`,
                                `/BIC/YPLANTYPE` AS `PLAN_TYPE`,
                                `/BIC/YPPERIOD` AS `PLANNED_PERIOD`,
                                `/BIC/YPVERSION` AS `PLAN_VERSION`,
                                `COMP_CODE` AS `COMPANY_CODE`,
                                `CO_AREA` AS `CONTROLLING_AREA`,
                                `/BIC/YSHREG` AS `REGION_SHIP_TO`,
                                `/BIC/YCOUNTRY` AS `COUNTRY_SHIP_TO`,
                                `RECTYPE` AS `RECORD_TYPE`,
                                `/BIC/YP_GVOL` AS `GROSS_VOLUME`,
                                `/BIC/YP_CAVOL` AS `VOLUME_IN_EACHES`,
                                `/BIC/YP_CVOL` AS `CAPTIVE_VOLUME`,
                                `/BIC/YP_GSV` AS `FC_GSV`,
                                `/BIC/YP_TPR` AS `TPR`,
                                `/BIC/YP_OTPR` AS `OTHER_TPR`,
                                `/BIC/YP_CP` AS `CP`,
                                `/BIC/YP_IFRS` AS `IFRS`,
                                `/BIC/YP_EXDTY` AS `EXCISE_DUTY`,
                                `/BIC/YP_ODED` AS `OTHER_DEDUCTIONS`,
                                `/BIC/YBSPACK`,
                                `CURRENCY`,
                                `/BIC/YCUSTMER`,
                                `/BIC/YCOUNTRY`,
                                `/BIC/YTDPNO`,
                                (`/BIC/YP_GVOL` - `/BIC/YP_CVOL`) AS `FC_VOL`
                               FROM FInantial_Forecast_Table""")


    Selected_DF.createOrReplaceTempView("Selected_Finantial_Forecast_Table")

    val Selected_join_dimProduct = spark.sql("""SELECT a.*,b.PRODUCT_KEY AS PRODUCT_KEY FROM Selected_Finantial_Forecast_Table AS a 
LEFT OUTER JOIN dimProduct_Table AS b ON b.BASEPACK_CODE = a.`/BIC/YBSPACK`""")

    Selected_join_dimProduct.createOrReplaceTempView("Selected_join_dimProduct_Table")

    val Selected_join_dimCurrency = spark.sql("""SELECT a.*,b.CURRENCY_KEY AS CURRENCY_KEY FROM Selected_join_dimProduct_Table AS a 
LEFT OUTER JOIN dimCurrency_Table AS b ON b.CURRENCY_NAME = a.`CURRENCY`""")

    Selected_join_dimCurrency.createOrReplaceTempView("Selected_join_dimCurrency_Table")

    /*val Selected_join_dimCustomer = spark.sql("""SELECT a.*,lpad(b.CUSTOMER_KEY,10,'0') AS CUSTOMER_KEY  FROM Selected_join_dimCurrency_Table AS a 
LEFT OUTER JOIN dimCustomer_Table AS b ON lpad(b.CUSTOMER_KEY,10,'0') = lpad(a.`/BIC/YCUSTMER`,10,'0')""")*/
    
    val Selected_join_dimCustomer = spark.sql("""SELECT * FROM Selected_join_dimCurrency_Table""").withColumn("CUSTOMER_KEY", lpad(col("/BIC/YCUSTMER"),10,"0"))

    Selected_join_dimCustomer.createOrReplaceTempView("Selected_join_dimCustomer_Table")

    val CurrentDt = CurrentDate()
    
    val Selected_join_dimCountry = spark.sql("""SELECT a.*,b.COUNTRY_KEY AS COUNTRY_KEY FROM Selected_join_dimCustomer_Table AS a 
LEFT OUTER JOIN dimCountry_Table AS b ON b.COUNTRY_NAME = a.`/BIC/YCOUNTRY`""")

    Selected_join_dimCountry.createOrReplaceTempView("Selected_join_dimCountry_Table")

  /*val Selected_join_dimTime = spark.sql("""SELECT a.*,b.TDP_START AS TIME_KEY, CONCAT(`PRODUCT_KEY`,'_',`CUSTOMER_KEY`) AS DIM_COMBINATION,'' AS UOM_KEY FROM Selected_join_dimCountry_Table AS a 
LEFT OUTER JOIN dimTime_Table AS b ON b.TDP = a.`/BIC/YTDPNO`""")*/
    
      val Selected_join_dimTime = spark.sql("""SELECT a.*,b.TDP_START AS TIME_KEY, CONCAT(`PRODUCT_KEY`,'_',`CUSTOMER_KEY`) AS DIM_COMBINATION,'' AS UOM_KEY FROM Selected_join_dimCountry_Table AS a 
LEFT OUTER JOIN (SELECT DISTINCT TDP_START,TDP,YEAR FROM dimTime_Table) AS b ON b.TDP = a.`/BIC/YTDPNO` and b.YEAR = a.`FISCYEAR`""")
  
Selected_join_dimTime.createOrReplaceTempView("FactTable")
dimTime_DF.createOrReplaceTempView("Date_Table")


//timeSpan_process(AggCol, CombinationColumn, DateColumn_Tup, spark)
timeSpan_process_temp(AggCol, CombinationColumn_Days,CombinationColumn_Other, DateColumn_Tup,("TDP_START",paramHashMap.getOrDefault("DateColumn_format", "NA")), spark)
/*val ResOp = spark.sql("SELECT * FROM op_table").withColumnRenamed("FC_GSV", "GSV")

ResOp.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination)*/

//val CurrentDt = CurrentDate()


if(!hdfs.isDirectory(new Path(Destination+"/Days")))
{
  hdfs.mkdirs(new Path(Destination+"/Days"))
}
spark.sql("SELECT * FROM res_factDF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Days")


if(!hdfs.isDirectory(new Path(Destination+"/Agg/MTD")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/MTD"))
  
}
spark.sql("SELECT * FROM res_MTD_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/MTD")

if(!hdfs.isDirectory(new Path(Destination+"/Agg/Months")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/Months"))
  
}
spark.sql("SELECT * FROM res_Month_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/Months")

if(!hdfs.isDirectory(new Path(Destination+"/Agg/QTD")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/QTD"))
  
}
spark.sql("SELECT * FROM res_QTD_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/QTD")

if(!hdfs.isDirectory(new Path(Destination+"/Agg/Year")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/Year"))
  
}
spark.sql("SELECT * FROM res_Year_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/Year")

if(!hdfs.isDirectory(new Path(Destination+"/Agg/YTD")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/YTD"))
  
}
spark.sql("SELECT * FROM res_YTD_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/YTD")

if(!hdfs.isDirectory(new Path(Destination+"/Agg/Quarter")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/Quarter"))
  
}
spark.sql("SELECT * FROM res_Quarter_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/Quarter")

if(!hdfs.isDirectory(new Path(Destination+"/Agg/L3M")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/L3M"))
  
}
spark.sql("SELECT * FROM res_L3M_DF").withColumnRenamed("FC_GSV", "GSV").withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/L3M")

if(!hdfs.isDirectory(new Path(Destination+"/Agg/MAT")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/MAT"))
  
}
spark.sql("SELECT * FROM res_MAT_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/MAT")


///////


if(!hdfs.isDirectory(new Path(Destination+"/Agg/MOC")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/MOC"))
  
}
spark.sql("SELECT * FROM res_MOC_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/MOC")

if(!hdfs.isDirectory(new Path(Destination+"/Agg/TDP")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/TDP"))
  
}
spark.sql("SELECT * FROM res_TDP_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/TDP")

if(!hdfs.isDirectory(new Path(Destination+"/Agg/L30D")))
{
  hdfs.mkdirs(new Path(Destination+"/Agg/L30D"))
  
}
spark.sql("SELECT * FROM res_LTD_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination+"/Agg/L30D")

  }
}