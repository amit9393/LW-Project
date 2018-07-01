package test

import org.apache.spark.sql.SparkSession
import landingtoudl.ResourceFunctions
import java.util.HashMap

object PDStest {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder().appName("PDStest").master("local[*]").getOrCreate()
    
    
    val ConfigFile = "C:/Users/a.jayendra.karekar/Desktop/LiveWire/PDStest.json"
    val DataSet = "TestPDS"
    val resource: ResourceFunctions = new ResourceFunctions()

    val paramHashMap: HashMap[String, String] = resource.getConfiguration(ConfigFile,DataSet, spark)
      
    val DataFiles = paramHashMap.getOrDefault("DataFiles", "NA")
    
    val Filepairs = DataFiles.split(",")
    
    for(filepair <- Filepairs)
    {
      val FilePath = filepair.split("#")(0)
      val TableName = filepair.split("#")(1)
      
      val DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(FilePath)
      
      DF.createOrReplaceTempView(TableName)
    }
      
    val keySet = paramHashMap.keySet()
    val keyite = keySet.iterator()
    
    while (keyite.hasNext()) {
      val key = keyite.next()
      if(!key.equals("DataFiles") && !key.equals("OutputFiles"))
      {
        val QueryDF = spark.sql(paramHashMap.get(key))
      
      QueryDF.createOrReplaceTempView(key)
      }
    }
    
    spark.sql("SELECT * FROM IntTable").show()
  }
}