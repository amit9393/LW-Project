package udlToPDS

import org.apache.spark.sql.SparkSession
import landingtoudl.ResourceFunctions
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import java.text.SimpleDateFormat
import java.util.HashMap
import org.apache.hadoop.fs.Path

object factsAlloc extends ResourceFunctions {
   def main(args: Array[String]): Unit = {
    
    //  var sb: StringBuffer = new StringBuffer("Inside the method: factSalesOrder.main \n")
      
       val spark = SparkSession.builder().appName("factSalesAllocation").config("spark.executor.cores",5).getOrCreate()
             import spark.implicits._
             
             
             
     val DataSet = "factSalesAllocation"
     val adlPath = args(0)
    val ConfigFile = args(1)
    
    
    
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(adlPath), hadoopConf)
    
    
    val paramHashMap: HashMap[String, String] = getConfiguration(ConfigFile,DataSet, spark)
    
    

 
    val salesrPath = adlPath + paramHashMap.getOrDefault("salesrPath", "NA")

    val dimTimePath = adlPath + paramHashMap.getOrDefault("dimTimePath", "NA")

    val dimProductPath = adlPath + paramHashMap.getOrDefault("dimProductPath", "NA")

    val dimCustomerPath = adlPath + paramHashMap.getOrDefault("dimCustomerPath", "NA")

    val dimCurrencyPath = adlPath + paramHashMap.getOrDefault("dimCurrencyPath", "NA")
    
    val dimCountryPath = adlPath + paramHashMap.getOrDefault("dimCountryPath", "NA")
	
	val CustomerPath = adlPath + paramHashMap.getOrDefault("CustomerPath", "NA")
	
	//val dimTimePeriodPath = adlPath + paramHashMap.getOrDefault("dimTimePeriodTypePath", "NA")
	
	val dimTimePeriod_path = adlPath + paramHashMap.getOrDefault("dimTimePeriod_path", "NA")
	    
    val Destination = adlPath + paramHashMap.getOrDefault("DestinationPath", "NA")
    
    
    
   val AggCol = paramHashMap.getOrDefault("AggCol", "NA").split('|')
   val CombinationColumn_Days = paramHashMap.getOrDefault("CombinationColumn", "NA").split(",")
   
   val CombinationColumn_Other = paramHashMap.getOrDefault("CombinationColumn_Other", "NA").split(",")
   
     val DateColumn_Tup = (paramHashMap.getOrDefault("DateColumn", "NA"),paramHashMap.getOrDefault("DateColumn_format", "NA"))
             
             
             
               
             val salesallocDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(salesrPath)
             val Cust_AttrDF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema","true").load(CustomerPath)
             val dimTimeDF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema","true").load(dimTimePath)
             val dimProductDF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema","true").load(dimProductPath)
             val dimCustomerDF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema","true").load(dimCustomerPath)
             val dimCurrencyDF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema","true").load(dimCurrencyPath)
             val dimCountryDF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema","true").load(dimCountryPath)
          //   val dimTimePeriodTypeDF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema","true").load(dimTimePeriodTypePath)
             val dimTimePeriod_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimTimePeriod_path)

             salesallocDF.createOrReplaceTempView("Sales_Table")
             dimTimeDF.createOrReplaceTempView("Time_Table")
              dimProductDF.createOrReplaceTempView("Product_Table")
              dimCustomerDF.createOrReplaceTempView("Customer_Table")
              dimCurrencyDF.createOrReplaceTempView("Currency_Table")
              dimCountryDF.createOrReplaceTempView("Country_Table")  
              Cust_AttrDF.createOrReplaceTempView("Cust_Attr_Table")
          //    dimTimePeriodTypeDF.createOrReplaceTempView("TimePeriodType_Table")
              dimTimePeriod_DF.createOrReplaceTempView("dimTimePeriod_Table")
      // sb.append("DOne with salesallocDF...\n")
  
              val DateConvert = ((DateField: String, InFormat: String, outFormat: String) => {
      var op = DateField
      try {
        val InputFormat = new java.text.SimpleDateFormat(InFormat)
        val OutputFormat = new java.text.SimpleDateFormat(outFormat)
        InputFormat.setLenient(false)
        if (InFormat.length() == DateField.length()) {
          val dt = InputFormat.parse(DateField)
          op = OutputFormat.format(dt)
        }
      } catch {
        case t: Throwable => { op = DateField }
      }
      op
    })                    

   spark.udf.register("DateFormatConvert", DateConvert)

  
 val Res_df = spark.sql("""SELECT          
                   Fcts.DELIV_NUMB DELIV_NO,
                   Fcts.SALES_UNIT SALES_UNIT,
                   Fcts.DOC_CURRCY DOCUMENT_CURRENCY,
                   Fcts.REQ_DATE DELIVERY_DATE,
                   Fcts.`/BIC/YCUSORD` CUSTOMER_ORDER,
                   Fcts.`/BIC/YCUSORDLN` CUSTOMER_ORDER_LINE,
                   Fcts.`/BIC/YSIPRS` SIP_AT_RS,
                   Fcts.`/BIC/YNWTINGRM` NET_WEIGHT_IN_GRAMS,
                   Fcts.`/BIC/YFBTDLQTY` BATCH_DETERMINE_QTY,  
                   cast(Fcts.`/BIC/YNWTINGRM` AS BIGINT)/1000 NET_WEIGHT_IN_KGS,           
                   Fcts.`/BIC/YFBTDLQTY` * Fcts.`/BIC/YSIPRS`  ALLOCATION_GSV,
                   CASE 
                   WHEN CAST(`/BIC/YNWTINGRM` AS BIGINT)=0 THEN (CAST(`/BIC/YFBTDLQTY` AS DECIMAL(19,6))* CAST(`/BIC/YNWTINGRM` AS BIGINT))
                   ELSE ((`/BIC/YFBTDLQTY` * `/BIC/YNWTINGRM`)/1000)
                   END  ALLOCATION_VOL,
                   CURRENT_DATE as CREATED_DATE,
                    'SPARK_ADMIN' AS CREATED_BY,
                     time.TIME_KEY TIME_KEY,
                    LPAD(Fcts.`/BIC/YMATERL`,18,0) as PRODUCT_KEY,
                    LPAD(Fcts.`/BIC/YCUSTMER`,10,0) as CUSTOMER_KEY,
                    Cntry.`COUNTRY_KEY` COUNTRY_KEY,
                    Curr.CURRENCY_KEY CURRENCY_KEY,
                    '' UOM_KEY, 
                    CONCAT( LPAD(Fcts.`/BIC/YMATERL`,18,0),'_',LPAD(Fcts.`/BIC/YCUSTMER`,10,0)) AS DIM_COMBINATION 
                    FROM Sales_Table Fcts
                    LEFT JOIN Time_Table time on Fcts.REQ_DATE=time.TIME_KEY
                    LEFT JOIN Currency_Table Curr ON Curr.`CURRENCY_NAME`= Fcts.DOC_CURRCY
                    LEFT JOIN Cust_Attr_Table CustAttr ON CustAttr.YCUSTMER=Fcts.`/BIC/YCUSTMER`
                    LEFT JOIN Country_Table Cntry ON CustAttr.Country=Cntry.`COUNTRY_NAME`
                    """)
    
                     
 
    Res_df.createOrReplaceTempView("FactTable")
    dimTimeDF.createOrReplaceTempView("Date_Table")

    timeSpan_process_temp(AggCol, CombinationColumn_Days, CombinationColumn_Other, DateColumn_Tup, ("TIME_KEY", paramHashMap.getOrDefault("DateColumn_format", "NA")), spark)

    val CurrentDt = CurrentDate()
    if (!hdfs.isDirectory(new Path(Destination + "/Days"))) {
      hdfs.mkdirs(new Path(Destination + "/Days"))
    }
    spark.sql("SELECT * FROM res_factDF_table").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Days")

    if (!hdfs.isDirectory(new Path(Destination + "/Agg/Months"))) {
      hdfs.mkdirs(new Path(Destination + "/Agg/Months"))

    }
    spark.sql("SELECT * FROM res_Month_DF_table").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/Months")

    if (!hdfs.isDirectory(new Path(Destination + "/Agg/YTD"))) {
      hdfs.mkdirs(new Path(Destination + "/Agg/YTD"))

    }
    spark.sql("SELECT * FROM res_YTD_DF_table").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/YTD")

    if (!hdfs.isDirectory(new Path(Destination + "/Agg/QTD"))) {
      hdfs.mkdirs(new Path(Destination + "/Agg/QTD"))

    }
    spark.sql("SELECT * FROM res_QTD_DF_table").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/QTD")

    if (!hdfs.isDirectory(new Path(Destination + "/Agg/Year"))) {
      hdfs.mkdirs(new Path(Destination + "/Agg/Year"))

    }
    spark.sql("SELECT * FROM res_Year_DF_table").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/Year")

    if (!hdfs.isDirectory(new Path(Destination + "/Agg/Quarter"))) {
      hdfs.mkdirs(new Path(Destination + "/Agg/Quarter"))

    }
    spark.sql("SELECT * FROM res_Quarter_DF_table").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/Quarter")

    if (!hdfs.isDirectory(new Path(Destination + "/Agg/L3M"))) {
      hdfs.mkdirs(new Path(Destination + "/Agg/L3M"))

    }
    spark.sql("SELECT * FROM res_L3M_DF").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/L3M")

    if (!hdfs.isDirectory(new Path(Destination + "/Agg/MAT"))) {
      hdfs.mkdirs(new Path(Destination + "/Agg/MAT"))

    }
    spark.sql("SELECT * FROM res_MAT_DF_table").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/MAT")

    if (!hdfs.isDirectory(new Path(Destination + "/Agg/MOC"))) {
      hdfs.mkdirs(new Path(Destination + "/Agg/MOC"))

    }
    spark.sql("SELECT * FROM res_MOC_DF_table").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/MOC")

   // Res_df.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination)

   
 
 }

}