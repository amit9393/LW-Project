package udlToPDS

import org.apache.spark.sql.SparkSession
import java.util.HashMap
import landingtoudl.ResourceFunctions

object factturachievement_draft extends ResourceFunctions{
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder().appName("factturachievement").config("spark.core.instance", "5").getOrCreate()
    import spark.implicits._
    
    val DataSet = "factturachievement"
    val adlPath = args(0)
    val ConfigFile = args(1)
    
    val sb:StringBuffer = new StringBuffer("Inside Main Method: factturachievement.main.....\n")
    
    val paramHashMap:HashMap[String,String] = getConfiguration(ConfigFile, DataSet, spark)
    
    val TUR_Sales_Path = adlPath + "/Unilever/UniversalDataLake/InternalSources/FusionBW/OpenHub/TURSales/India"
    val Customer_Path = adlPath+ "/Unilever/UniversalDataLake/InternalSources/FusionBW/OpenHub/Customer/India"
    val Dimpricelisthierarchy_Path = adlPath + "/Unilever/ProductDataStores/LiveWire/India/Dimensions/dimPriceList"
    val Destination_Path = adlPath + "/Unilever/ProductDataStores/LiveWire/India/facts/factturachievement"

   /* val TUR_Sales_Path = adlPath + paramHashMap.getOrDefault("TUR_Sales_Path", "NA")
    val Customer_Path = adlPath+ paramHashMap.getOrDefault("Customer_Path", "NA")
    val Dimpricelisthierarchy_Path = adlPath + paramHashMap.getOrDefault("Dimpricelisthierarchy_Path", "NA")
    val time_Path =  adlPath + paramHashMap.getOrDefault("time_Path", "NA")
    val Destination_Path = adlPath + paramHashMap.getOrDefault("Destination_Path", "NA")
    */
    //val TUR_SalesDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(TUR_Sales_Path)
    
    val TUR_SalesDF = spark.read.option("header", "true").csv(TUR_Sales_Path)
    val CustomerDF = spark.read.option("header", "true").csv(Customer_Path)
    val DimpricelisthierarchyDF =  spark.read.option("header", "true").csv(Dimpricelisthierarchy_Path)
//    val timeDF = spark.read.option("header", "true").csv(time_Path)
    
    val aggCol = paramHashMap.getOrDefault("aggCol", "NA").split('|')
    val CombinationColumn = paramHashMap.getOrDefault("CombinationColumn", "NA").split(",")
    val DateColumn_Tup = (paramHashMap.getOrDefault("DateColumn", "NA"), paramHashMap.getOrDefault("DateColumn_format", "NA"))
    
    TUR_SalesDF.createOrReplaceTempView("TUR_Sales")
    CustomerDF.createOrReplaceTempView("Customer")
    DimpricelisthierarchyDF.createOrReplaceTempView("Dimpricelisthierarchy")
    
    
    val Res_DF = spark.sql("""select CALDAY as TIME_KEY,
         YCUSTMER as CUSTOMER,
        YPRICELST as PRICE_LIST,
        YSKU7 as SKU7,
        YMOCTOT as MOC_TOT
    		from TUR_Sales TUR
    		where TUR.YPRICELST in 
    		(SELECT trim(Pricelist_code) FROM Dimpricelisthierarchy   WHERE Dimpricelisthierarchy.Price_list_group  IN ('DETS','FNB','PP'))
    		and 
    		TUR.YCUSTMER in
    		( select YCUSTMER from Customer where Where Customer.YCUSCHNL in ('GT','RD') """)
    
    		
    		
    Res_DF.createOrReplaceTempView("FactTable")
    //timeDF.createOrReplaceTempView("Date_Table")
    
    timeSpan_process(aggCol, CombinationColumn, DateColumn_Tup, spark)
    
    val Res_Op = spark.sql("select * from op_table")
    Res_Op.write.option("header","true").mode("overwrite").csv(Destination_Path)
    
    
    
    
    
    
  }
}