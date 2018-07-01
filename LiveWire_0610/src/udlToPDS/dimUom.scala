package udlToPDS

import landingtoudl.ResourceFunctions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.HashMap

object dimUom extends ResourceFunctions {
  def main(args: Array[String]): Unit = {

    var sb: StringBuffer = new StringBuffer("In dimUOM main method........\n")

    sb.append("Starting Spark.............\n")

    val spark = SparkSession.builder().appName("dimUOM").getOrCreate()
    import spark.implicits._

    /*val DataSet = ""
    val adlPath = ""
    val ConfigFile = "adl://bienodad57179stgadls.azuredatalakestore.net/Unilever/Testing/Configuration/Transaction_Configuration_Amesh_20180511.json"
    */

    val DataSet = "dimUom"
    val adlPath = args(0)
    val ConfigFile = args(1)

    val paramHashMap: HashMap[String, String] = getConfiguration(ConfigFile, DataSet, spark)

    val SourceFile = adlPath + paramHashMap.getOrDefault("SourcePath", "NA")

    sb.append("SourceFile: " + SourceFile + "\n")

    val Destination = adlPath + paramHashMap.getOrDefault("Destination", "NA")

    sb.append("Destination: " + Destination + "\n")

    sb.append("Reading csv file from source.......\n")
    val InpDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(SourceFile)

    sb.append("Creating view Inp_Table.......\n")
    InpDF.createOrReplaceTempView("Inp_Table")

    sb.append("Actual logic.......\n")
    val ResDF_temp = spark.sql("SELECT FROM_UNIT AS FROM_UNIT, TO_UNIT AS TO_UNIT, COMPONENT AS COMPONENT, CONVERSION_FACTOR AS CONVERSION_FACTOR FROM Inp_Table")

    val GroupDF = ResDF_temp.groupBy("TO_UNIT", "COMPONENT").agg(count("*").as("COUNT_STAR")).filter("`COUNT_STAR` = 1").filter("`TO_UNIT` is not null AND `COMPONENT` is not null")

    val ResDF = GroupDF.as("D2").join(ResDF_temp.as("D1"), GroupDF("TO_UNIT") === ResDF_temp("TO_UNIT") && GroupDF("COMPONENT") === ResDF_temp("COMPONENT")).select($"D1.*")
    ResDF.createOrReplaceTempView("ResDF_Table")

    val FinalDF = spark.sql("SELECT row_number() over (order by TO_UNIT,COMPONENT) as CONVERSION_ID,* FROM ResDF_Table")

    sb.append("Writing file to Target Directory......\n")
    FinalDF.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination)

    println(sb.toString())
  }
}