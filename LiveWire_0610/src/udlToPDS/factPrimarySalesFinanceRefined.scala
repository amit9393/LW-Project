package udlToPDS

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import java.text.SimpleDateFormat
import landingtoudl.ResourceFunctions
import java.util.HashMap

object factPrimarySalesFinanceRefined extends ResourceFunctions {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("factPrimarySalesFinanceRefined").config("spark.executor.cores", 5).getOrCreate()

    import spark.implicits._

    val DataSet = "factPrimarySalesFinanceRefined"
    val adlPath = args(0)
    val ConfigFile = args(1)

    var sb: StringBuffer = new StringBuffer("Inside the method: factPrimarySalesFinanceRefined.main \n")

    val paramHashMap: HashMap[String, String] = getConfiguration(ConfigFile, DataSet, spark)

    val actuals_waterPath = adlPath + paramHashMap.getOrDefault("actuals_waterPath", "NA")

    val HCPC_Lakme_ActualPath = adlPath + paramHashMap.getOrDefault("HCPC_Lakme_ActualPath", "NA")

    val HCPC_Without_LakmeActualPath = adlPath + paramHashMap.getOrDefault("HCPC_Without_LakmeActualPath", "NA")

    val Foods_ActualPath = adlPath + paramHashMap.getOrDefault("Foods_ActualPath", "NA")

    val dimProductPath = adlPath + paramHashMap.getOrDefault("dimProductPath", "NA")
    val dimCustomerPath = adlPath + paramHashMap.getOrDefault("dimCustomerPath", "NA")
    val dimCountryPath = adlPath + paramHashMap.getOrDefault("dimCountryPath", "NA")
    val dimTimePeriodTypePath = adlPath + paramHashMap.getOrDefault("dimTimePeriodTypePath", "NA")
    val CustomerPath = adlPath + paramHashMap.getOrDefault("CustomerPath", "NA")
    val dimTimePath = adlPath + paramHashMap.getOrDefault("dimTimePath", "NA")

    val DestinationPath = adlPath + paramHashMap.getOrDefault("DestinationPath", "NA")

    val AggCol = paramHashMap.getOrDefault("AggCol", "NA").split('|')
    val CombinationColumn = paramHashMap.getOrDefault("CombinationColumn", "NA").split(",")
    val DateColumn_Tup = (paramHashMap.getOrDefault("DateColumn", "NA"), paramHashMap.getOrDefault("DateColumn_format", "NA"))

    val actuals_waterDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(actuals_waterPath)
    val HCPC_Lakme_ActualDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(HCPC_Lakme_ActualPath)
    val HCPC_Without_LakmeActualDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(HCPC_Without_LakmeActualPath)
    val Foods_ActualDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Foods_ActualPath)
    val Cust_AttrDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(CustomerPath)
    val dimProductDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimProductPath)
    val dimCustomerDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimCustomerPath)
    val dimCountryDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimCountryPath)
    val dimTimePeriodTypeDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimTimePeriodTypePath)
    val dimTimeDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimTimePath)

    actuals_waterDF.createOrReplaceTempView("actuals_water_Table")

    HCPC_Lakme_ActualDF.createOrReplaceTempView("HCPC_Lakme_Actual_Table")
    HCPC_Without_LakmeActualDF.createOrReplaceTempView("HCPC_Without_LakmeActual_Table")
    Foods_ActualDF.createOrReplaceTempView("Foods_Actual_Table")
    dimProductDF.createOrReplaceTempView("Product_Table")
    dimCustomerDF.createOrReplaceTempView("Customer_Table")
    dimCountryDF.createOrReplaceTempView("Country_Table")
    Cust_AttrDF.createOrReplaceTempView("Cust_Attr_Table")
    dimTimePeriodTypeDF.createOrReplaceTempView("dimTimePeriod_Table")
    //Time_Period_Table
    dimTimeDF.createOrReplaceTempView("Time_Table")

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
			SELECT HPL.FISCAL_YEAR,HPL.BRANCH_DESC,HPL.BRANCH_CODE,HPL.CLUSTER_DESC,HPL.CLUSTER_CODE,HPL.STATE,HPL.DISTRIBUTION_CHANNEL,HPL.WPC_PROFIT_CENTER,PR.BASEPACK_CODE,HPL.SALES_VOLUMNE,HPL.GSV_LOCAL,HPL.TTS_LOCAL,HPL.TURN_OVER,HPL.GROSS_PROFIT,HPL.GROSS_MARGIN,HPL.TOTAL_BRAND,HPL.PROFIT_BEFORE_OVER,HPL.GROSS_GSV,HPL.GROSS_VOLUME,HPL.PROCESS_TIMESTAMP
			FROM HCPC_Lakme_Actual_Table HPL
			LEFT JOIN Product_Table PR ON PR.BASEPACK=HPL.BASEPACK 
			""")
    TM1_DF.createOrReplaceTempView("TM1_Table")

    val Res_df = spark.sql("""SELECT 
			time.FIRST_DAY_MONTH TIME_KEY,
			FR.SALES_VOLUMNE SALES_VOL,
			FR.GSV_LOCAL GSV_LOCAL,
			FR.TTS_LOCAL TTS_LOCAL,
			FR.TURN_OVER TURN_OVER,
			FR.GROSS_PROFIT GROSS_PROFIT,
			FR.GROSS_MARGIN GROSS_MARGIN,
			FR.TOTAL_BRAND TOTAL_BRAND,
			FR.PROFIT_BEFORE_OVER PROFIT_BEFORE_OVER,
			FR.GROSS_GSV GROSS_GSV,
			FR.GROSS_VOLUME GROSS_VOLUME,
	//		LPAD(P.PRODUCT_KEY,18,0) PRODUCT_KEY,
      LPAD(FR.BASEPACK,18,0) PRODUCT_KEY,
	//		LPAD(C.CUSTOMER_KEY,10,0) CUSTOMER_KEY,
      LPAD(C.CUSTOMER_KEY,10,0) CUSTOMER_KEY,
			Cust.COUNTRY_KEY COUNTRY_KEY,
			CONCAT(LPAD(P.PRODUCT_KEY,18,0),'_',LPAD(C.CUSTOMER_KEY,10,0)) DIM_COMBINATION,
      '' 3P_SALES_VOL,
      '' 3P_TURNOVER,
      '' TOTAL_BR_MKT_INV,
			'' UOM_KEY,
			'' CURRENCY_KEY,
			FR.FISCAL_YEAR MONTH_YEAR
			FROM TM1_Table FR
	//		LEFT JOIN Product_Table P ON P.BASEPACK_CODE=FR.BASEPACK AND PRIMARY_MATERIAL='1'
	//		LEFT JOIN Customer_Table C ON C.BRANCH=FR.BRANCH_CODE AND PRIMARY_CUSTOMER_BRANCH='1' 
			LEFT JOIN (select distinct CustAttr.YBRANCH,CustAttr.Country,Cntry.COUNTRY_KEY from Cust_Attr_Table CustAttr
			JOIN Country_Table Cntry ON CustAttr.Country=Cntry.COUNTRY_NAME) Cust ON Cust.YBRANCH=FR.BRANCH_CODE
			LEFT JOIN (SELECT DISTINCT FIRST_DAY_MONTH,CALENDAR_MONTH FROM  Time_Table) time ON time.CALENDAR_MONTH = DateConvert(FISCAL_YEAR,'MMM_yy','yyyyMM')
      where Cust.YBRANCH IN ('B001','B002','B003','B004','B010')
			""")


    Res_df.createOrReplaceTempView("FactTable")
    dimTimeDF.createOrReplaceTempView("Date_Table")

    timeSpan_process(AggCol, CombinationColumn, DateColumn_Tup, spark)

    val ResOp = spark.sql("SELECT * FROM op_table")

    //ResOp.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(DestinationPath)
    //Res_df.write.format("com.databricks.spark.csv").option("header","true").option( "treatEmptyValuesAsNulls" ,"true").mode("overwrite").save(DestinationPath)
    ResOp.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(DestinationPath)
    //Res_df.show(20)

  }

}
