package udlToPDS

import org.apache.spark.sql.SparkSession
import java.util.HashMap
import landingtoudl.ResourceFunctions

object factsecondarysales extends ResourceFunctions{
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder().appName("factsecondarysales").config("spark.executor.cores", 5).getOrCreate()
    import spark.implicits._
    
    val DataSet = "factsecondarysales"
    val adlPath =  args(0)
    val ConfigPath = args(1)
    
    val paramHashMap:HashMap[String,String] = getConfiguration(ConfigPath, DataSet, spark)
    
    val Secondary_Sales_U1_Sales_Path = adlPath + paramHashMap.getOrDefault("Secondary_Sales_U2_Sales_Path", "NA")
    val Secondary_Sales_U1_Returns_Path = adlPath + paramHashMap.getOrDefault("Secondary_Sales_U1_Returns_Path", "NA")
    val Secondary_Sales_U2_Sales_Path = adlPath + paramHashMap.getOrDefault("Secondary_Sales_U2_Sales_Path", "NA")
    val Secondary_Sales_U2_Returns_Path = adlPath + paramHashMap.getOrDefault("Secondary_Sales_U2_Returns_Path", "NA")
    
    val DestinationPath = adlPath + paramHashMap.getOrDefault("DestinationPath", "NA")
    val AggCol = paramHashMap.getOrDefault("AggCol", "NA").split('|')
    val CombinationColumn = paramHashMap.getOrDefault("CombinationColumn", "NA").split(",")
    val DateColumn_Tup = (paramHashMap.getOrDefault("DateColumn", "NA"), paramHashMap.getOrDefault("DateColumn_format", "NA"))
    
    val Secondary_Sales_U1_SalesDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Secondary_Sales_U1_Sales_Path)
    val Secondary_Sales_U1_ReturnsDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Secondary_Sales_U1_Returns_Path)
    val Secondary_Sales_U2_SalesDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Secondary_Sales_U2_Sales_Path)
    val Secondary_Sales_U2_ReturnsDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Secondary_Sales_U2_Returns_Path)

    Secondary_Sales_U1_SalesDF.createOrReplaceTempView("Secondary_Sales_U1")
    Secondary_Sales_U1_ReturnsDF.createOrReplaceTempView("Secondary_Sales_U1_Returns")
    Secondary_Sales_U2_SalesDF.createOrReplaceTempView("Secondary_Sales_U2_Sales")
    Secondary_Sales_U2_ReturnsDF.createOrReplaceTempView("Secondary_Sales_U2_Returns")
    
    
    
    

    
    
  }
}