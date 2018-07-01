package udlToPDS

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import java.text.SimpleDateFormat
import java.util.Calendar;
import java.util.Date
import java.util.GregorianCalendar
import org.apache.spark.sql.catalyst.expressions.Concat
import landingtoudl.ResourceFunctions
import java.util.HashMap

object dimTimePeriod extends ResourceFunctions{
  def main(args: Array[String]): Unit = {
   // val sourcePath = "adl://bienodad57179stgadls.azuredatalakestore.net/Unilever/UniversalDataLake/InternalSources/ManualFiles/OnPremFileShare/TimePeriod/LandedFiles/BAU/TimePeriodType*.csv"

   // val DestinationPath = "adl://bienodad57179stgadls.azuredatalakestore.net/Unilever/ProductDataStores/LiveWire/India/dimTimePeriod"

     var sb = new StringBuffer
    try {
      val DataSet = "dimTimePeriod"
      val adlPath = args(0)
      val ConfigFile = args(1)
      val spark = SparkSession.builder().appName("dimTimePeriod").getOrCreate()
      import spark.implicits._

      val paramHashMap: HashMap[String, String] = getConfiguration(ConfigFile, DataSet, spark)
      sb.append("paramhashMap: " + paramHashMap + "\n")

      val Source_Path = adlPath + paramHashMap.getOrDefault("Source_Path", "NA")
      sb.append("Source_Path: " + Source_Path + "\n")
    
      val Destination = adlPath + paramHashMap.getOrDefault("Destination", "NA")
      sb.append("Destination: " + Destination + "\n")
    
    val CurrentDt = CurrentDate()

    val Schema = StructType(Array(StructField("TIMEPERIOD_TYPE", StringType, true),
      StructField("TIMEPERIOD_TYPE_ID", StringType, true)))
   //val Spark=SparkSession.builder().appName("Hello").master("local[*]").getOrCreate()

    val InpDF = spark.read.format("com.databricks.spark.csv").option("header", "true").schema(Schema).load(Source_Path)
    InpDF.createOrReplaceTempView("Inp_Table")

    val GroupDF = spark.sql("SELECT `TIMEPERIOD_TYPE`, COUNT(*) AS `COUNT_STAR` FROM Inp_Table GROUP BY TIMEPERIOD_TYPE").filter("`COUNT_STAR` = 1").filter("`TIMEPERIOD_TYPE` is not null")

    val ResDF = GroupDF.join(InpDF.as("D1"), GroupDF("TIMEPERIOD_TYPE") === InpDF("TIMEPERIOD_TYPE"))
                       .withColumn("CREATE_DT", lit(CurrentDt))
                       .withColumn("MODIFIED_DT", lit(CurrentDt))
                       .withColumn("CREATED_BY", lit("SPARK_ADMIN"))
                       .withColumn("MODIFIED_BY", lit("SPARK_ADMIN"))
                       .select($"D1.*", $"CREATE_DT", $"MODIFIED_DT", $"CREATED_BY", $"MODIFIED_BY")

  val hadoopConf = new org.apache.hadoop.conf.Configuration()
      val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(Destination), hadoopConf)

      if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(Destination))) {
        sb.append("Creating a new Destination dir : " + Destination + "\n")
        hdfs.mkdirs(new org.apache.hadoop.fs.Path(Destination))
      }

      // <-- Build complete for creating a destination path.

      ResDF.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination)

    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
    println(sb.toString())

  }
} 