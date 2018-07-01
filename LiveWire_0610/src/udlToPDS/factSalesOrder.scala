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

object factSalesOrder extends ResourceFunctions {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("factSalesOrder").config("spark.executor.cores", 5).getOrCreate()
    import spark.implicits._

    val DataSet = "factSalesOrder"
    val adlPath = args(0)
    val ConfigFile = args(1)

    println("starting config file")

    val paramHashMap: HashMap[String, String] = getConfiguration(ConfigFile, DataSet, spark)

    println("Done with config file")

    val order_path = adlPath + paramHashMap.getOrDefault("order_path", "NA")

    val dimProduct_path = adlPath + paramHashMap.getOrDefault("dimProduct_path", "NA")

    val dimCurrency_path = adlPath + paramHashMap.getOrDefault("dimCurrency_path", "NA")

    val dimCustomer_path = adlPath + paramHashMap.getOrDefault("dimCustomer_path", "NA")

    val dimCountry_path = adlPath + paramHashMap.getOrDefault("dimCountry_path", "NA")

    val dimTime_path = adlPath + paramHashMap.getOrDefault("dimTime_path", "NA")

    val dimTimePeriodType_path = adlPath + paramHashMap.getOrDefault("dimTimePeriodType_path", "NA")

    val CustomerAtttr_path = adlPath + paramHashMap.getOrDefault("CustomerAtttr_path", "NA")

    val Destination = adlPath + paramHashMap.getOrDefault("Destination", "NA")

    val AggCol = paramHashMap.getOrDefault("AggCol", "NA").split('|')
    val CombinationColumn = paramHashMap.getOrDefault("CombinationColumn", "NA").split(",")
    val DateColumn_Tup = (paramHashMap.getOrDefault("DateColumn", "NA"), paramHashMap.getOrDefault("DateColumn_format", "NA"))

    var sb: StringBuffer = new StringBuffer("Inside the method: factSalesOrder.main \n")

    val orderDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(order_path)
    val Cust_AttrDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(CustomerAtttr_path)
    val dimTimeDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimTime_path)
    val dimProductDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimProduct_path)
    val dimCustomerDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimCustomer_path)
    val dimCurrencyDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimCurrency_path)
    val dimCountryDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimCountry_path)
    val dimTimePeriodTypeDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dimTimePeriodType_path)

    orderDF.createOrReplaceTempView("Order_Table")
    dimTimeDF.createOrReplaceTempView("Time_Table")
    dimProductDF.createOrReplaceTempView("Product_Table")
    dimCustomerDF.createOrReplaceTempView("Customer_Table")
    dimCurrencyDF.createOrReplaceTempView("Currency_Table")
    dimCountryDF.createOrReplaceTempView("Country_Table")
    Cust_AttrDF.createOrReplaceTempView("Cust_Attr_Table")
    dimTimePeriodTypeDF.createOrReplaceTempView("dimTimePeriod_Table")

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

    sb.append("DOne with orderDF...\n")

    val Ordr_df = spark.sql("""
        select * FROM Order_Table Ordr   
        WHERE (Ordr.DEL_BLOCK NOT IN ('00','03','04','05','08','09','10','11','50','AA','AB','AO','AQ','AU','AY','BC','BF','BG','XC','Y1','Y2','Y3','Y6','Y8','Y9','YL','YM','YN','YP','YQ','YR','YS','YU','YW','YX','YY','YZ','Z1','Z2','Z3','Z4','Z6','Z7','Z8','ZA','ZB','ZC','ZD','ZG','ZI','ZJ','ZK','ZO','ZQ','ZU','ZV','ZW') or Ordr.DEL_BLOCK IS NULL) 
  			 AND
	  		 Ordr.DOC_TYPE IN ('ZL07','ZL28','ZRR1','ZRR2','','ZRR3','ZRR4','ZS07','ZS08','ZS20','ZS28','ZS70','ZS71','ZS74','ZS87')
		  	 AND
			   (Ordr.REASON_REJ NOT IN ('00','03','04','05','08','09','10','11','50','AA','AB','AO','AQ','AU','AY','BC','BF','BG','XC','Y1','Y2','Y3','Y6','Y8','Y9','YL','YM','YN','YP','YQ','YR','YS','YU','YW','YX','YY','YZ','Z1','Z2','Z3','Z4','Z6','Z7','Z8','ZA','ZB','ZC','ZD','ZG','ZI','ZJ','ZK','ZO','ZQ','ZU','ZV','ZW') or Ordr.REASON_REJ IS NULL)
  			 AND
	  		 Ordr.`/BIC/YITCATCLS` IN ('HBH','LBP','NOR')
		  	 AND
			   Ordr.`/BIC/YCUSORDLN` IS NOT NULL
         AND
         Ordr.`/BIC/YCUSORD` IS NOT NULL
         AND
         Ordr.COMP_CODE in ('H','I','LLPL')
      """)
      
    Ordr_df.createOrReplaceTempView("Temp_Order_Table")

    val Res_df = spark.sql("""SELECT 
			 Ordr.DOC_NUMBER SALES_DOCUMENT,
			 Ordr.STORNO REV_IND,
			 Ordr.REJECTN_ST REJECTION_STATUS,
			 Ordr.CML_OR_QTY ORDER_QUANTITY,
			 Ordr.SALES_UNIT SALES_UNIT,
			 Ordr.DOC_CURRCY DOCUMENT_CURRENCY,
			 LPAD(Ordr.`/BIC/YSHIPTO`,10,0)  HLL_SHIP_TO_PARTY,
			 Ordr.PO_UNIT ORDER_UNIT,
			 Ordr.`/BIC/YALOCQTY` ALLOCATED_QTY_SU,
			 Ordr.BILL_DATE DELIVERY_DATE,
			 Ordr.`/BIC/YCUSORD` CUSTOMER_ORDER,
			 LPAD(Ordr.`/BIC/YCUSORDLN`,6,0) CUSTOMER_ORDER_LINE,
			 Ordr.`/BIC/YNWTINKGS` NET_WEIGHT_IN_KGS,
			 Ordr.`/BIC/YSIPRS` SIP_AT_RS,
			 Ordr.`/BIC/YNWTINGRM` NET_WEIGHT_IN_GRAMS,
			 Ordr.`/BIC/YREPDATE` REPORTING_DATE,
			 (CAST(Ordr.CML_OR_QTY AS DECIMAL(19,6))+CAST(Ordr.`/BIC/YALOCQTY` AS DECIMAL(19,6)))*ABS(CAST(Ordr.`/BIC/YSIPRS` AS DECIMAL(19,6))) ORDER_GSV,
			 CASE WHEN CAST(Ordr.`/BIC/YNWTINGRM` AS INT)==0 THEN (CAST(Ordr.CML_OR_QTY AS DECIMAL(19,6))+CAST(Ordr.`/BIC/YALOCQTY` AS DECIMAL(19,6)))*CAST(Ordr.`/BIC/YNWTINKGS` AS DECIMAL(19,6))
			 ELSE (Ordr.CML_OR_QTY+Ordr.`/BIC/YALOCQTY`)*Ordr.`/BIC/YNWTINGRM`/1000 END  ORDER_VOL,
       time.TIME_KEY TIME_KEY,
			 Prod.PRODUCT_KEY PRODUCT_KEY,
			 LPAD(Cust.CUSTOMER_KEY,10,0) CUSTOMER_KEY,
			 Curr.CURRENCY_KEY CURRENCY_KEY,
			 country_temp.COUNTRY_KEY COUNTRY_KEY,
			 '' UOM_KEY,
      CONCAT(Prod.PRODUCT_KEY,'_',LPAD(Cust.CUSTOMER_KEY,10,0)) DIM_COMBINATION
			 FROM Temp_Order_Table Ordr   
       LEFT JOIN Time_Table time on time.TIME_KEY = Ordr.REQ_DATE
			 LEFT JOIN Customer_Table Cust ON Cust.`CUSTOMER_KEY`=Ordr.`/BIC/YCUSTMER`
			 LEFT JOIN Currency_Table Curr ON Curr.`CURRENCY_NAME`=Ordr.LOC_CURRCY
			 LEFT JOIN (select distinct YCUSTMER,Country,Cntry.COUNTRY_KEY from Cust_Attr_Table CustAttr  JOIN 
       (select distinct COUNTRY_NAME,COUNTRY_KEY from Country_Table) Cntry
        ON CustAttr.Country=Cntry.`COUNTRY_NAME`) country_temp
        ON country_temp.YCUSTMER=Ordr.`/BIC/YCUSTMER` 			 
        LEFT JOIN (SELECT distinct LPAD(PRODUCT_KEY,18,0) as PRODUCT_KEY from Product_Table) Prod ON Prod.PRODUCT_KEY=LPAD(Ordr.`/BIC/YMATERL`,18,0)
     """)

    Res_df.createOrReplaceTempView("FactTable")
    dimTimeDF.createOrReplaceTempView("Date_Table")

    timeSpan_process(AggCol, CombinationColumn, DateColumn_Tup, spark)

    val ResOp = spark.sql("SELECT * FROM op_table")
    ResOp.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination)

  }
}