package udlToPDS

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import landingtoudl.ResourceFunctions
import java.util.HashMap

object dimCurrency extends ResourceFunctions {
  def main(args: Array[String]): Unit = {
    
     val DataSet = "dimCurrency"
     val adlPath = args(0)
    val ConfigFile = args(1)

     val spark = SparkSession.builder().appName("dimCurrency").getOrCreate()
    import spark.implicits._
    
    val paramHashMap: HashMap[String, String] = getConfiguration(ConfigFile,DataSet, spark)

    val sourcePath = adlPath+paramHashMap.getOrDefault("SourcePath", "NA")

    val Destination = adlPath+paramHashMap.getOrDefault("Destination", "NA")
    
    val Schema = StructType(Array(StructField("CURRENCY_KEY", StringType, true),
      StructField("CURRENCY_NAME", StringType, true),
      StructField("CURRENCY_DESCRIPTION", StringType, true)))

     val CurrentDt = CurrentDate()

    val InpDF = spark.read.format("com.databricks.spark.csv").option("header", "true").schema(Schema).load(sourcePath)
    InpDF.createOrReplaceTempView("Inp_Table")

    val GroupDF = spark.sql("SELECT `CURRENCY_KEY`, COUNT(*) AS `COUNT_STAR` FROM Inp_Table GROUP BY CURRENCY_KEY").filter("`COUNT_STAR` = 1").filter("`CURRENCY_KEY` is not null")

    val ResDF = GroupDF.join(InpDF.as("D1"), GroupDF("CURRENCY_KEY") === InpDF("CURRENCY_KEY"))
                       .withColumn("CREATE_DT", lit(CurrentDt))
                       .withColumn("MODIFIED_DT", lit(CurrentDt))
                       .withColumn("CREATED_BY", lit("SPARK_ADMIN"))
                       .withColumn("MODIFIED_BY", lit("SPARK_ADMIN"))
                       .select($"D1.*", $"CREATE_DT", $"MODIFIED_DT", $"CREATED_BY", $"MODIFIED_BY")
                       
                       
    // -- Build for creating a destination path, if not available -->
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(Destination), hadoopConf)

    if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(Destination))) {
      hdfs.mkdirs(new org.apache.hadoop.fs.Path(Destination))
    }

    // <-- Build complete for creating a destination path.

    ResDF.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination)
  }
}