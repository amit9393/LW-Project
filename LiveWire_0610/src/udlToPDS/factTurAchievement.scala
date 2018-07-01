package udlToPDS

import org.apache.spark.sql.SparkSession
import java.util.HashMap
import landingtoudl.ResourceFunctions
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import java.text.SimpleDateFormat

object factTurAchievement extends ResourceFunctions {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("factTurAchievement").config("spark.core.instance", "5").getOrCreate()
    import spark.implicits._

    val DataSet = "factTurAchievement"
    val adlPath = args(0)
    val ConfigFile = args(1)

    val sb: StringBuffer = new StringBuffer("Inside Main Method: factturachievement.main.....\n")

    val paramHashMap: HashMap[String, String] = getConfiguration(ConfigFile, DataSet, spark)

    /*    val TUR_Sales_Path = adlPath + "/Unilever/UniversalDataLake/InternalSources/FusionBW/OpenHub/TURSales/India"
    val Customer_Path = adlPath + "/Unilever/UniversalDataLake/InternalSources/FusionBW/OpenHub/Customer/India"
    val Dimpricelisthierarchy_Path = adlPath + "/Unilever/ProductDataStores/LiveWire/India/Dimensions/dimPriceList"
    val Destination_Path = adlPath + "/Unilever/ProductDataStores/LiveWire/India/facts/factturachievement"
    val time_Path = adlPath + paramHashMap.getOrDefault("time_Path", "NA")*/

    val TUR_Sales_Path = adlPath + paramHashMap.getOrDefault("TUR_Sales_Path", "NA")
    val Customer_Path = adlPath + paramHashMap.getOrDefault("Customer_Path", "NA")
    val Dimpricelisthierarchy_Path = adlPath + paramHashMap.getOrDefault("Dimpricelisthierarchy_Path", "NA")
    val Destination = adlPath + paramHashMap.getOrDefault("Destination_Path", "NA")
    

    //val TUR_SalesDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(TUR_Sales_Path)

    val CombinationColumn_Days = paramHashMap.getOrDefault("CombinationColumn_Days", "NA").split(",")
    val CombinationColumn_Other = paramHashMap.getOrDefault("CombinationColumn_Other", "NA").split(",")

    val AggCol = paramHashMap.getOrDefault("AggCol", "NA").split('|')
    
    val dimTimePath = adlPath + paramHashMap.getOrDefault("dimTimePath", "NA")
    val dimTimePeriodTypePath = adlPath + paramHashMap.getOrDefault("dimTimePeriodTypePath", "NA")
    
    //val TUR_SalesDF = spark.read.option("header", "true").csv(TUR_Sales_Path)
    //val CustomerDF = spark.read.option("header", "true").csv(Customer_Path)
    //val DimpricelisthierarchyDF = spark.read.option("header", "true").csv(Dimpricelisthierarchy_Path)
    
    val TUR_SalesDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(TUR_Sales_Path)
    val CustomerDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Customer_Path)
    val DimpricelisthierarchyDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Dimpricelisthierarchy_Path)
    val dimTimePeriodTypeDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimTimePeriodTypePath)
    val dimTimeDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimTimePath)

    val aggCol = paramHashMap.getOrDefault("aggCol", "NA").split('|')
    val CombinationColumn = paramHashMap.getOrDefault("CombinationColumn_Days", "NA").split(",")
    val DateColumn_Tup = (paramHashMap.getOrDefault("DateColumn", "NA"), paramHashMap.getOrDefault("DateColumn_format", "NA"))

    TUR_SalesDF.createOrReplaceTempView("TUR_Sales")
    CustomerDF.createOrReplaceTempView("Customer")
    DimpricelisthierarchyDF.createOrReplaceTempView("Dimpricelisthierarchy")
    dimTimePeriodTypeDF.createOrReplaceTempView("dimTimePeriod_Table")
    dimTimeDF.createOrReplaceTempView("Time_Table")

/*    val Res_DF = spark.sql("""select 
    		0CALDAY as TIME_KEY,
    		YMOCMNTH1 as MOC_MONTH,
    		YCUSTMER as CUSTOMER,
    		YPRICELST as PRICE_LIST,
    		YSKU7 as SKU7,
    		YMOCTOT as MOC_TOT
    		from TUR_Sales TUR
    		where TUR.YPRICELST in 
    		(SELECT trim(Pricelist_code) FROM Dimpricelisthierarchy WHERE Dimpricelisthierarchy.Price_list_group IN ('DETS','FNB','PP'))
    		and 
    		TUR.YCUSTMER in 
    		(SELECT YCUSTMER from Customer where Where Customer.YCUSCHNL in ('GT','RD')) """)*/
    
     val DateConvert = ((DateField: String, InFormat: String, outFormat: String) => {
      var op = DateField
      try {
        val InputFormat = new SimpleDateFormat(InFormat)
        val OutputFormat = new SimpleDateFormat(outFormat)
        InputFormat.setLenient(false);
        if (InFormat.length() == DateField.length()) {
          val dt = InputFormat.parse(DateField)
          op = OutputFormat.format(dt)
        }
      } catch {
        case t: Throwable => { op = DateField }
      }
      op
    })

    val DateFormatConvert = spark.udf.register("DateConvert", DateConvert)
    		
  
    /*val Res_DF = spark.sql("""(select 
    		`0CALDAY` as TIME_KEY,
    		YMOCMNTH1 as MOC_MONTH,
    		YCUSTMER as CUSTOMER,
    		YPRICELST as PRICE_LIST,
    		YSKU7 as SKU7,
    		YMOCTOT as MOC_TOT
    		from TUR_Sales TUR
          LEFT JOIN Time_Table time on time.TIME_KEY = TUR.`0CALDAY` 
    		where TUR.YPRICELST in
    		(SELECT trim(Pricelist_code) FROM Dimpricelisthierarchy WHERE Dimpricelisthierarchy.Price_list_group IN ('DETS','FNB','PP'))
    		and
    		TUR.YCUSTMER in
    		( select YCUSTMER from Customer Where Customer.YCUSCHNL in ('GT','RD') ))
        """) */
 
    //DateConvert(time.FIRST_DAY_MONTH,'ddMMMyyyy','yyyyMMdd') as TIME_KEY,
    
   val Res_DF = spark.sql("""(select 
    		`0CALDAY` as TIME_KEY,
    		LPAD(YCUSTMER,10,0) as CUSTOMER,
    		YPRICELST as PRICE_LIST,
    		YSKU7 as SKU7,
    		YMOCTOT as MOC_TOT
    		from TUR_Sales TUR
    		where TUR.YPRICELST in
    		(SELECT trim(Pricelist_code) FROM Dimpricelisthierarchy WHERE Dimpricelisthierarchy.Price_list_group IN ('DETS','FNB','PP'))
    		and
    		TUR.YCUSTMER in
    		( select YCUSTMER from Customer Where Customer.YCUSCHNL in ('GT','RD') ))
        """)  


    Res_DF.createOrReplaceTempView("FactTable")
    dimTimeDF.createOrReplaceTempView("Date_Table")

    /*timeSpan_process(aggCol, CombinationColumn, DateColumn_Tup, spark)

    val Res_Op = spark.sql("select * from op_table")
    Res_Op.write.option("header", "true").mode("overwrite").csv(Destination)*/
    
   
    timeSpan_process_temp(AggCol, CombinationColumn_Days, CombinationColumn_Other, DateColumn_Tup, ("TIME_KEY", paramHashMap.getOrDefault("DateColumn_format", "NA")), spark)
    
    

    //val ResOp = spark.sql("SELECT * FROM op_table")

    //ResOp.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(DestinationPath)
    //Res_df.write.format("com.databricks.spark.csv").option("header","true").option( "treatEmptyValuesAsNulls" ,"true").mode("overwrite").save(DestinationPath)
    //ResOp.repartition(50).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(DestinationPath)
    //Res_df.show(20)

    val CurrentDt = CurrentDate()
    spark.sql("SELECT * FROM res_factDF_table").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Days")
    spark.sql("SELECT * FROM res_MTD_DF_table").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/MTD")
    spark.sql("SELECT * FROM res_Month_DF_table").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/Months")
    spark.sql("SELECT * FROM res_YTD_DF_table").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/YTD")
    spark.sql("SELECT * FROM res_QTD_DF_table").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/QTD")
    spark.sql("SELECT * FROM res_Year_DF_table").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/Year")
    spark.sql("SELECT * FROM res_Quarter_DF_table").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/Quarter")
    spark.sql("SELECT * FROM res_L3M_DF").write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/L3M")
    spark.sql("SELECT * FROM res_MAT_DF_table").repartition(30).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/MAT")
    /*spark.sql("SELECT * FROM res_factDF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).repartition(30).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Days")
    spark.sql("SELECT * FROM res_MTD_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).repartition(30).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/MTD")
    spark.sql("SELECT * FROM res_Month_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).repartition(30).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/Months")
    spark.sql("SELECT * FROM res_YTD_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).repartition(30).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/YTD")
    spark.sql("SELECT * FROM res_QTD_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).repartition(30).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/QTD")
    spark.sql("SELECT * FROM res_Year_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).repartition(30).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/Year")
    spark.sql("SELECT * FROM res_Quarter_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).repartition(30).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/Quarter")
    spark.sql("SELECT * FROM res_L3M_DF").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).repartition(30).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/L3M")
    spark.sql("SELECT * FROM res_MAT_DF_table").withColumnRenamed("FC_GSV", "GSV").withColumn("MODIFIED_DATE", lit(CurrentDt)).withColumn("MODIFIED_BY", lit("SPARK_ADMIN")).withColumn("CREATED_DATE", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN")).repartition(30).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination + "/Agg/MAT")*/

  }
}