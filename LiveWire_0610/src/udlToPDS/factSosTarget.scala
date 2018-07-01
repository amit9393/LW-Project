package udlToPDS

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import java.text.SimpleDateFormat
import java.util.HashMap
import landingtoudl.ResourceFunctions

object factSosTarget extends ResourceFunctions {
  def main(args: Array[String]): Unit = {
    
     val spark = SparkSession.builder().appName("factSosTarget").config("spark.executor.cores", 5).getOrCreate()

    import spark.implicits._

    val DataSet = "factSosTarget"
    val adlPath = args(0)
    val ConfigFile = args(1)

    var sb: StringBuffer = new StringBuffer("Inside the method: factSalesOrder.main \n")
     
   val paramHashMap: HashMap[String, String] = getConfiguration(ConfigFile,DataSet, spark)

    val SOS_TARGET_Path = adlPath + paramHashMap.getOrDefault("SOS_TARGET_Path", "NA")
    val time_Path = adlPath + paramHashMap.getOrDefault("time_Path", "NA")
    val mapping_path = adlPath + paramHashMap.getOrDefault("mapping_path", "NA")
    val dimTimePeriodTypePath = adlPath + paramHashMap.getOrDefault("dimTimePeriodTypePath", "NA")
    
    val DestinationPath = adlPath + paramHashMap.getOrDefault("DestinationPath", "NA")
    
    val AggCol = paramHashMap.getOrDefault("AggCol", "NA").split('|')
    val CombinationColumn = paramHashMap.getOrDefault("CombinationColumn", "NA").split(",")
    val DateColumn_Tup = (paramHashMap.getOrDefault("DateColumn", "NA"), paramHashMap.getOrDefault("DateColumn_format", "NA"))
    
    val SOS_TARGET_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(SOS_TARGET_Path)
    val time_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(time_Path)
    val mapping_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(mapping_path)
    val dimTimePeriodTypeDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimTimePeriodTypePath)

    SOS_TARGET_DF.createOrReplaceTempView("SOS_TARGET_Table")
    time_DF.createOrReplaceTempView("time_Table")
    mapping_DF.createOrReplaceTempView("mapping_Table")
    dimTimePeriodTypeDF.createOrReplaceTempView("dimTimePeriod_Table")
    
    val DateConvert = ((DateField: String, InFormat: String, outFormat: String) => {
      var op = DateField
      try {
        val InputFormat = new SimpleDateFormat(InFormat)
        val OutputFormat = new SimpleDateFormat(outFormat)
        InputFormat.setLenient(false);
        val dt = InputFormat.parse(DateField)
        op = OutputFormat.format(dt)

      } catch {
        case t: Throwable => { op = DateField }
      }
      op
    })

   spark.udf.register("DateConvert", DateConvert)
   
   val Res_DF = spark.sql("""select SOS.`Actual Spends` as ACTUAL_SPENDS,
		   DateConvert(SOS.`Time Period`,'dd/MM/yyyy','yyyyMMdd') as TIME_KEY,
		   mapping.`Livewire Cluster` as CLUSTER_KEY,
		   SOS.`Brand Variant` as MEDIA_PRODUCT_KEY,
		   SOS.Media as MEDIA_KEY
		   FROM SOS_TARGET_Table SOS
		   left join (SELECT DISTINCT FIRST_DAY_MONTH FROM  time_Table) time
		   on DateConvert(SOS.`Time Period`,'dd/MM/yyyy','yyyyMMdd') = DateConvert(time.FIRST_DAY_MONTH,'ddMMMyyyy','yyyyMMdd')
		   left join mapping_Table mapping
		   on SOS.Cluster = mapping.`BMI Cluster`
		   """)
		   
    Res_DF.createOrReplaceTempView("FactTable")
    time_DF.createOrReplaceTempView("Date_Table")

    timeSpan_process(AggCol, CombinationColumn, DateColumn_Tup, spark)

    val ResOp = spark.sql("SELECT * FROM op_table")
		   
    ResOp.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(DestinationPath)
    
    
  }
  
}