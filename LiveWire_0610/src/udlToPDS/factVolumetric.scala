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


object factVolumetric extends ResourceFunctions {
 def main(args: Array[String]): Unit = {
    var sb: StringBuffer = new StringBuffer("Inside the method: factVolumetric.main \n")
     
   val spark = SparkSession.builder().appName("factVolumetric").config("spark.executor.cores",5).getOrCreate()
             import spark.implicits._
           
     val DataSet = "factVolumetric"
     val adlPath = args(0)
    val ConfigFile = args(1)
    
    val paramHashMap: HashMap[String, String] = getConfiguration(ConfigFile,DataSet, spark)

    val factVolumetric_path = adlPath + paramHashMap.getOrDefault("factVolumetric_path", "NA")

    val dimTime_Path = adlPath + paramHashMap.getOrDefault("dimTime_Path", "NA")

    val BvMapping_Path = adlPath + paramHashMap.getOrDefault("BvMapping_Path", "NA")

    val BmiMapping_Path = adlPath + paramHashMap.getOrDefault("BmiMapping_Path", "NA")
    
	val dimTimePeriod_path = adlPath + paramHashMap.getOrDefault("dimTimePeriod_path", "NA")

    val DestinationPath = adlPath + paramHashMap.getOrDefault("DestinationPath", "NA")
    
    val AggCol = paramHashMap.getOrDefault("AggCol", "NA").split('|')
    val CombinationColumn = paramHashMap.getOrDefault("CombinationColumn", "NA").split(",")
    val DateColumn_Tup = (paramHashMap.getOrDefault("DateColumn", "NA"),paramHashMap.getOrDefault("DateColumn_format", "NA"))
             
    
   
val factVolumetric_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(factVolumetric_path)
val dimTime_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(dimTime_Path)
val BvMapping_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(BvMapping_Path)
val BmiMapping_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(BmiMapping_Path)
val dimTimePeriod_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimTimePeriod_path)



factVolumetric_DF.createOrReplaceTempView("factVolumetri_table")
dimTime_DF.createOrReplaceTempView("dimTime_table")
BvMapping_DF.createOrReplaceTempView("BvMapping_table")
BmiMapping_DF.createOrReplaceTempView("BmiMapping_table")
dimTimePeriod_DF.createOrReplaceTempView("dimTimePeriod_Table")
  
   
   sb.append("DOne with factVolumetric...\n")
   
   
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

 val resDF = spark.sql("""SELECT
                   cast(fav.`Value Offtake(000 Rs.)`as DECIMAL(38,6)) as VALUE_OFFTAKE,
                   cast(fav.`Sales Volume (Volume(LITRES))` as DECIMAL(38,6)) as SALES_VOL,
                   cast(fav.`Gr Val YA` as DECIMAL(38,6)) AS GR_VAL_YA,
                   coalesce(trim(bmi.Phase_1_Cluster),'NESA') as CLUSTER_KEY,
                   DateConvert(TIMEPERIOD,'MMMyy','yyyyMMdd') as TIME_KEY,
                   coalesce(trim(bvm.Brand_Variant),'LAKME 9 TO 5 CC NOURISHING CREAM') as  MEDIA_PRODUCT_KEY,
                   concat(concat(coalesce(trim(bvm.Brand_Variant),'LAKME 9 TO 5 CC NOURISHING CREAM'),'_'),coalesce(trim(bmi.Phase_1_Cluster),'NESA')) as combination_code,
                   CURRENT_DATE as CREATED_DATE,
                   'SPARK_ADMIN' AS CREATED_BY
                   FROM factVolumetri_table fav
                   LEFT JOIN BmiMapping_table bmi on fav.Markets = bmi.BMI_Cluster
                   LEFT JOIN BvMapping_table bvm on fav.BRAND = bvm.P1_Brand and fav.Category = bvm.P1_Cat and fav.Manufacturer = bvm.P1_MF
                   where fav.Period='Month'  
                  """)
                  
                  
                     resDF.createOrReplaceTempView("FactTable")
                     dimTime_DF.createOrReplaceTempView("Date_Table")
                     
                     
                     timeSpan_process(AggCol, CombinationColumn, DateColumn_Tup, spark)
                     
                     
                     val ResOp = spark.sql("SELECT * FROM op_table")
                     
                     
                    //  ResOp.coalesce(1)
                      
                      
                      ResOp.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(DestinationPath)

  // resDF.show()
                  
                  
                 
                  
     //       resDF.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(DestinationPath)      
   
   
  }
}