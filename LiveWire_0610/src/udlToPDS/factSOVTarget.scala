package udlToPDS

import org.apache.spark.sql.SparkSession
import landingtoudl.ResourceFunctions
import java.util.HashMap
import java.text.SimpleDateFormat

object factSovTarget extends ResourceFunctions {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("factSOVTarget").config("spark.executor.cores", 5).getOrCreate()

    import spark.implicits._

    val DataSet = "factSOVTarget"
    val adlPath = args(0)
    val ConfigFile = args(1)

    var sb: StringBuffer = new StringBuffer("Inside the method: factSOVTarget.main \n")
    val paramHashMap: HashMap[String, String] = getConfiguration(ConfigFile, DataSet, spark)

    val sovTargetUdlPath = adlPath + paramHashMap.getOrDefault("sovTargetUdlPath", "NA")
    val dimTimePath = adlPath + paramHashMap.getOrDefault("dimTimePath", "NA")
    val BMItoPhase1Path = adlPath + paramHashMap.getOrDefault("BMItoPhase1Path", "NA")
    val dimTimePeriodTypePath = adlPath + paramHashMap.getOrDefault("dimTimePeriodTypePath", "NA")
    

    val DestinationPath = adlPath + paramHashMap.getOrDefault("DestinationPath", "NA")
    val AggCol = paramHashMap.getOrDefault("AggCol", "NA").split('|')
    val CombinationColumn = paramHashMap.getOrDefault("CombinationColumn", "NA").split(",")
    val DateColumn_Tup = (paramHashMap.getOrDefault("DateColumn", "NA"), paramHashMap.getOrDefault("DateColumn_format", "NA"))

    val sovTargetDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(sovTargetUdlPath)
    val dimTimeDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimTimePath)
    val BMItoPhase1DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(BMItoPhase1Path)
    val dimTimePeriodTypeDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimTimePeriodTypePath)

    sovTargetDF.createOrReplaceTempView("SOV_Target")
    dimTimeDF.createOrReplaceTempView("dimTime")
    BMItoPhase1DF.createOrReplaceTempView("BMItoPhase1")
    dimTimePeriodTypeDF.createOrReplaceTempView("dimTimePeriod_Table")

    val Res_df = spark.sql("""
    select sovTar.`30 Sec Norm Grp` as 30_SEC_NORM_GRP,
    sovTar.`Targets` as TARGETS,
    time.FIRST_DAY_MONTH as TIME_KEY,
    BP1.`Livewire Cluster` as CLUSTER_KEY,
    sovTar.`Brand Variant` as MEDIA_PRODUCT_KEY,
    sovTar.`Media` as MEDIA_KEY,
    sovTar.`Week` as WEEK from SOV_Target sovTar
    LEFT JOIN BMItoPhase1 BP1 on sovTar.Cluster = BP1.`BMI Cluster`
    LEFT JOIN (SELECT DISTINCT FIRST_DAY_MONTH, MONTH_NAME, YEAR FROM dimTime) time 
    on (sovTar.Month = SUBSTRING(time.MONTH_NAME,0,3) and sovTar.Year = time.YEAR)
    """)

    Res_df.createOrReplaceTempView("FactTable")
    dimTimeDF.createOrReplaceTempView("Date_Table")

    timeSpan_process(AggCol, CombinationColumn, DateColumn_Tup, spark)

    val ResOp = spark.sql("SELECT * FROM op_table")

    ResOp.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(DestinationPath)

  }
}