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



object factSPONT {
  def main(args: Array[String]): Unit = {
    var sb: StringBuffer = new StringBuffer("Inside the method: factSPONT.main \n")
     
   val In_Spont_QTR_path = "/Unilever/UniversalDataLake/ExternalSources/MB/OnPremFileShare/SpontaneousAwarnessQTR/India/Month"
   val dimTime_Path = "/Unilever/ProductDataStores/LiveWire/India/Dimensions/dimTime/part-00000-213df199-4e3f-4ed1-83b6-9a3717b1d548.csv"
   val Spont_07032017_v1_Path = ""
   val DestinationPath = "/Unilever/ProductDataStores/LiveWire/India/factSPONT"
   
    val spark = SparkSession.builder().appName("factSPONT").getOrCreate()
   import spark.implicits._
   
   
 val In_Spont_QTR_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(In_Spont_QTR_path)
val dimTime_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(dimTime_Path)
val Spont_07032017_v1_DF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(Spont_07032017_v1_Path)

  In_Spont_QTR_DF.createOrReplaceTempView("In_Spont_QTR_table")
  In_Spont_QTR_DF.createOrReplaceTempView("dimTime_table")
  In_Spont_QTR_DF.createOrReplaceTempView("Spont_07032017_v1_table")
   
   sb.append("DOne with factSPONTDF...\n")
   
   
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
                    spQtr.SpontQuarterly SPONT,
                    DateConvert(time.FIRST_DAY_QUARTER,'ddMMMyyyy','yyyyMMdd') as TIME_KEY,
                    spv1.Mindshare Brands  MEDIA_PRODUCT_KEY, 
                    spQtr.MARKET MARKET_KEY,
                    CREATED_DATE,
                    CREATED_BY FROM In_Spont_QTR_table spQtr
                  LEFT JOIN Spont_07032017_v1_table spv1 on spQtr.Brand=UPPER(spv1.Spont Brands)
                  left join (SELECT DISTINCT FIRST_DAY_QUARTER FROM  dimTime_table) time
                  on DateConvert(spQtr.SpontQuarterly`,'dd/MM/yyyy','yyyyMMdd') = DateConvert(time.CALENDAR_QUARTER,'ddMMMyyyy','yyyyMMdd')+'_'+DateConvert(time.YEAR,'ddMMMyyyy','yyyyMMdd')
                     """)

   resDF.show()
   
   
  }
}
