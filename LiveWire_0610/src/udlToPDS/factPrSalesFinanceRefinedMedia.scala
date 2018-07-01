package udlToPDS

import landingtoudl.ResourceFunctions
import org.apache.spark.sql.SparkSession
import java.util.HashMap
import java.text.SimpleDateFormat

object factPrSalesFinanceRefinedMedia extends ResourceFunctions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("factPrimarySalesFinanceRefined").config("spark.executor.cores", 5).getOrCreate()

    import spark.implicits._

    val DataSet = "factPrSalesFinanceRefinedMedia"
    val adlPath = args(0)
    val ConfigFile = args(1)

    var sb: StringBuffer = new StringBuffer("Inside the method: factPrimarySalesFinanceRefined.main \n")

    val paramHashMap: HashMap[String, String] = getConfiguration(ConfigFile, DataSet, spark)

    val actuals_waterPath = adlPath + paramHashMap.getOrDefault("actuals_waterPath", "NA")
    val HCPC_Lakme_ActualPath = adlPath + paramHashMap.getOrDefault("HCPC_Lakme_ActualPath", "NA")
    val HCPC_Without_LakmeActualPath = adlPath + paramHashMap.getOrDefault("HCPC_Without_LakmeActualPath", "NA")
    val Foods_ActualPath = adlPath + paramHashMap.getOrDefault("Foods_ActualPath", "NA")
    val dimTimePath = adlPath + paramHashMap.getOrDefault("dimTimePath", "NA")
    val dimTimePeriodTypePath = adlPath + paramHashMap.getOrDefault("dimTimePeriodTypePath", "NA")
    val BmiTm1BasepackMedia = adlPath + paramHashMap.getOrDefault("BmiTm1BasepackMedia", "NA")
    val Tm1BmiMediaCluster = adlPath + paramHashMap.getOrDefault("Tm1BmiMediaCluster", "NA")
    val DestinationPath = adlPath + paramHashMap.getOrDefault("DestinationPath", "NA")
    val AggCol = paramHashMap.getOrDefault("AggCol", "NA").split('|')
    val CombinationColumn = paramHashMap.getOrDefault("CombinationColumn", "NA").split(",")
    val DateColumn_Tup = (paramHashMap.getOrDefault("DateColumn", "NA"), paramHashMap.getOrDefault("DateColumn_format", "NA"))

    val actuals_waterDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(actuals_waterPath)
    val HCPC_Lakme_ActualDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(HCPC_Lakme_ActualPath)
    val HCPC_Without_LakmeActualDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(HCPC_Without_LakmeActualPath)
    val Foods_ActualDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Foods_ActualPath)
    val dimTimeDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimTimePath)
    val dimTimePeriodTypeDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimTimePeriodTypePath)
    val BmiTm1BasepackMediaDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(BmiTm1BasepackMedia)
    val Tm1BmiMediaClusterDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Tm1BmiMediaCluster)

    dimTimeDF.createOrReplaceTempView("dimTime")
    dimTimePeriodTypeDF.createOrReplaceTempView("dimTimePeriod_Table")
    actuals_waterDF.createOrReplaceTempView("actuals_water_Table")
    HCPC_Lakme_ActualDF.createOrReplaceTempView("HCPC_Lakme_Actual_Table")
    HCPC_Without_LakmeActualDF.createOrReplaceTempView("HCPC_Without_LakmeActual_Table")
    Foods_ActualDF.createOrReplaceTempView("Foods_Actual_Table")
    BmiTm1BasepackMediaDF.createOrReplaceTempView("BMI_TM1_Basepack_Media_Table")
    Tm1BmiMediaClusterDF.createOrReplaceTempView("TM1_BMI_Media_Cluster_Table")

    val DateConvert = ((DateField: String, InFormat: String, outFormat: String) => {
      var op = DateField
      try {
        val InputFormat = new SimpleDateFormat(InFormat)
        val OutputFormat = new SimpleDateFormat(outFormat)
        InputFormat.setLenient(false);
        if (InFormat.length() == DateField.length()) {
          val dt = InputFormat.parse(DateField)
          op = OutputFormat.format(dt)
        }
      } catch {
        case t: Throwable => { op = DateField }
      }
      op
    })

    val DateFormatConvert = spark.udf.register("DateConvert", DateConvert)

    val TM1_DF = spark.sql("""
			SELECT * FROM Foods_Actual_Table
			UNION ALL
			SELECT * FROM HCPC_Without_LakmeActual_Table
			UNION ALL
			SELECT * FROM actuals_water_Table
			UNION ALL
			SELECT * FROM HCPC_Lakme_Actual_Table HPL
			""")
    TM1_DF.createOrReplaceTempView("TM1_Table")

    val Res_df = spark.sql("""SELECT 
    		FR.SALES_VOLUMNE SALES_VOL,
    		TB.cluster_Corrected CLUSTER_KEY,
    		BT.Media_Brand MEDIA_PRODUCT_KEY,
    		CONCAT(TB.cluster_Corrected,'_',BT.Media_Brand) DIM_COMBINATION,
    		FR.GSV_LOCAL GSV_LOCAL,
    		FR.TTS_LOCAL TTS_LOCAL,
    		time.FIRST_DAY_MONTH as TIME_KEY,
    		FR.TURN_OVER TURN_OVER,
    		FR.GROSS_PROFIT GROSS_PROFIT,
    		FR.GROSS_MARGIN GROSS_MARGIN,
    		FR.TOTAL_BRAND TOTAL_BRAND,
    		FR.PROFIT_BEFORE_OVER PROFIT_BEFORE_OVER,
    		FR.GROSS_GSV GROSS_GSV,
    		FR.GROSS_VOLUME GROSS_VOLUME,
    		FR.FISCAL_YEAR MONTH_YEAR
    		FROM TM1_Table FR 
    		LEFT JOIN TM1_BMI_Media_Cluster_Table TB
    		ON TB.TM1_State = FR.STATE
    		LEFT JOIN BMI_TM1_Basepack_Media_Table BT
    		ON BT.BP_Code = FR.BASEPACK
    		LEFT JOIN (SELECT DISTINCT FIRST_DAY_MONTH, CALENDAR_MONTH FROM dimTime) time 
    		ON time.CALENDAR_MONTH = DateConvert(FR.FISCAL_YEAR,'MMM_yy','yyyyMM')
    		""")

    Res_df.createOrReplaceTempView("FactTable")
    dimTimeDF.createOrReplaceTempView("Date_Table")

    timeSpan_process(AggCol, CombinationColumn, DateColumn_Tup, spark)

    val ResOp = spark.sql("SELECT * FROM op_table")

    ResOp.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(DestinationPath)

  }

}