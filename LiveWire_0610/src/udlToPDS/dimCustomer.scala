package udlToPDS

import landingtoudl.ResourceFunctions
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.HashMap

object dimCustomer extends ResourceFunctions {
  def main(args: Array[String]): Unit = {

    var sb: StringBuffer = new StringBuffer("In dimCustomer main method........\n")

    val DataSet = "dimCustomer"
    val adlPath = args(0)
    val ConfigFile = args(1)

    val spark = SparkSession.builder().appName("dimCustomer").getOrCreate()
    import spark.implicits._
    sb.append("Created Spark.............\n")

    val paramHashMap: HashMap[String, String] = getConfiguration(ConfigFile, DataSet, spark)

    val actuals_waterPath = adlPath + paramHashMap.getOrDefault("actuals_waterPath", "NA")
    val HCPC_Lakme_ActualPath = adlPath + paramHashMap.getOrDefault("HCPC_Lakme_ActualPath", "NA")
    val HCPC_Without_LakmeActualPath = adlPath + paramHashMap.getOrDefault("HCPC_Without_LakmeActualPath", "NA")
    val Foods_ActualPath = adlPath + paramHashMap.getOrDefault("Foods_ActualPath", "NA")
    val RegionPath = adlPath + paramHashMap.getOrDefault("RegionPath", "NA")
    val BW_TM1Path = adlPath + paramHashMap.getOrDefault("BW_TM1Path", "NA")
    val Customer_MappingPath = adlPath + paramHashMap.getOrDefault("Customer_MappingPath", "NA")
    val TM1_MappingPath = adlPath + paramHashMap.getOrDefault("TM1_MappingPath", "NA")
    val customerPath = adlPath + paramHashMap.getOrDefault("customerPath", "NA")
    val customerTextPath = adlPath + paramHashMap.getOrDefault("customerTextPath", "NA")
    val DistributionChannelTextPath = adlPath + paramHashMap.getOrDefault("DistributionChannelTextPath", "NA")
    val CustomerHeirarchyU1Path = adlPath + paramHashMap.getOrDefault("CustomerHeirarchyU1Path", "NA")
    val CustomerHeirarchyAllSalesPath = adlPath + paramHashMap.getOrDefault("CustomerHeirarchyAllSalesPath", "NA")
    val CustomerHierarchyU2Path = adlPath + paramHashMap.getOrDefault("CustomerHierarchyU2Path", "NA")
    val BranchNameTextPath = adlPath + paramHashMap.getOrDefault("BranchNameTextPath", "NA")
    val ClusterCodeTextPath = adlPath + paramHashMap.getOrDefault("ClusterCodeTextPath", "NA")
    val DestinationPath = adlPath + paramHashMap.getOrDefault("DestinationPath", "NA")

    sb.append("Reading Customer,CustomerText,DistributionChannelText,CustomerHeirarchy,BranchNameText and clusterCodeText file.............\n")
    val actuals_waterDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(actuals_waterPath)
    val HCPC_Lakme_ActualDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(HCPC_Lakme_ActualPath)
    val HCPC_Without_LakmeActualDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(HCPC_Without_LakmeActualPath)
    val Foods_ActualDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Foods_ActualPath)

    val RegionDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(RegionPath)
    val BW_TM1DF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(BW_TM1Path)

    val Customer_MappingDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Customer_MappingPath)
    val TM1_MappingDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(TM1_MappingPath)

    val CustomerDF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(customerPath)
    val CustomerTextDF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(customerTextPath)
    val DistributionChannelTextDF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(DistributionChannelTextPath)
    val CustomerHeirarchyU1DF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(CustomerHeirarchyU1Path)
    val CustomerHeirarchyAllSalesDF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(CustomerHeirarchyAllSalesPath)
    val CustomerHierarchyU2DF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(CustomerHierarchyU2Path)
    val BranchNameTextDF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(BranchNameTextPath)
    val ClusterCodeTextDF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(ClusterCodeTextPath)

    actuals_waterDF.createOrReplaceTempView("actuals_water_Table")
    HCPC_Lakme_ActualDF.createOrReplaceTempView("HCPC_Lakme_Actual_Table")
    HCPC_Without_LakmeActualDF.createOrReplaceTempView("HCPC_Without_LakmeActual_Table")
    Foods_ActualDF.createOrReplaceTempView("Foods_Actual_Table")

    RegionDF.createOrReplaceTempView("Region_Table")
    BW_TM1DF.createOrReplaceTempView("BW_TM1_Table")

    Customer_MappingDF.createOrReplaceTempView("Customer_Mapping_Table")
    TM1_MappingDF.createOrReplaceTempView("TM1_Mapping_Table")

    CustomerDF.createOrReplaceTempView("Customer_Table")
    CustomerTextDF.createOrReplaceTempView("CustomerText_Table")
    DistributionChannelTextDF.createOrReplaceTempView("DistributionChannelTextTable")
    CustomerHeirarchyU1DF.createOrReplaceTempView("CustomerHeirarchyU1_Table")
    CustomerHeirarchyAllSalesDF.createOrReplaceTempView("CustomerHeirarchyAllSales_Table")
    CustomerHierarchyU2DF.createOrReplaceTempView("CustomerHierarchyU2_Table")
    BranchNameTextDF.createOrReplaceTempView("BranchNameText_Table")
    ClusterCodeTextDF.createOrReplaceTempView("ClusterCodeText_Table")

    val TM1_DF = spark.sql("""
							SELECT * FROM Foods_Actual_Table
							UNION ALL
							SELECT * FROM HCPC_Without_LakmeActual_Table
							UNION ALL
							SELECT * FROM actuals_water_Table
							UNION ALL
							SELECT *	FROM HCPC_Lakme_Actual_Table
							""")
    TM1_DF.createOrReplaceTempView("TM1_Table")

    sb.append("Spark SQL Query to apply the logic for dimCustomer.............\n")
    
    

    val dimCustomerDf_temp = spark.sql("""SELECT LPAD(CUST.YCUSTMER,10,0) Customer_KEY,
                                              CUST.YBRANCH BRANCH,
                                              BRANCHTXT.TXTSH BRANCH_NAME,
                                              CUST.YCLUSTCD CLUSTER,
                 CLUSTTXT.TXTSH CLUSTER_NAME,
                 CUSTTXT.TXTSH CUST_NAME,
                 CUST.DISTR_CHAN DISTRIBUTION_CHANNEL,
                 DISTTXT.TXTSH DISTR_CH_NAME,
                 '' DEPO,
                 '' SALES_AREA,
                 '' SALES_ORGANIZATION,
                 '' GEOGRAPHY,
                 '' MEDIA_CLUSTER,
                 '' STATE,
                 P.YTERRTRY TERRITORY,
                 P.YSRS_CUST SALES_RS,
                 P.YPOPCLA POPSTRATA,
                 P.YZONE ZONE,
                 CUST.YDISTRCT DISTRICT,
                 CUST.YTOWN TOWN,
                 CUST.CUST_GROUP CUSTOMER_GROUP,
                 CUST.CUST_GRP4 CUSTOMER_GROUP4,
                 '' DATASOURCE_NAME
                FROM Customer_Table CUST
                LEFT JOIN BranchNameText_Table BRANCHTXT ON CUST.YBRANCH=BRANCHTXT.YBRANCH
                LEFT JOIN CustomerText_Table CUSTTXT ON CUST.YCUSTMER=CUSTTXT.YCUSTOMER
                LEFT JOIN ClusterCodeText_Table CLUSTTXT ON CLUSTTXT.YCLUSTCD=CUST.YCLUSTCD
                LEFT JOIN DistributionChannelTextTable  DISTTXT ON DISTTXT.DISTR_CHAN=CUST.DISTR_CHAN
                LEFT JOIN(SELECT YCUSTMER,YPOPCLA,YZONE,YTERRTRY,YSRS_CUST FROM CustomerHeirarchyAllSales_Table 
                 UNION
                 SELECT YCUSTMER,YPOPCLA,YZONE,YTERRTRY,YSRS_CUST FROM CustomerHeirarchyU1_Table
                 WHERE YCUSTMER NOT IN(SELECT YCUSTMER FROM CustomerHeirarchyAllSales_Table)
                 UNION 
                 SELECT YCUSTMER,YPOPCLA,YZONE,YTERRTRY,YSRS_CUST FROM CustomerHierarchyU2_Table
                 WHERE YCUSTMER NOT IN (SELECT YCUSTMER FROM  CustomerHeirarchyAllSales_Table
												                      UNION
													                    SELECT YCUSTMER FROM CustomerHeirarchyU1_Table
													 )
                                              )P ON P.YCUSTMER=CUST.YCUSTMER
                              """)

    dimCustomerDf_temp.createOrReplaceTempView("dimCustomer_temp_Table")

    val dimCustomer_Df = spark.sql(""" SELECT * ,
								CASE WHEN ROW_NUMBER()OVER(PARTITION BY DISTRIBUTION_CHANNEL ORDER BY Customer_KEY)=1 THEN '1' ELSE '0' END PRIMARY_CUSTOMER,
								CASE WHEN ROW_NUMBER()OVER(PARTITION BY BRANCH ORDER BY Customer_KEY)=1 THEN '1' ELSE '0' END PRIMARY_CUSTOMER_BRANCH
								FROM  dimCustomer_temp_Table""")

    dimCustomer_Df.createOrReplaceTempView("dimCustomer_Table")

    val cust_DF = spark.sql("""select CUST.*,coalesce(map_TM1.Channel,map_Cust.Channel) as CHANNEL
          		from dimCustomer_Table CUST
                  left join 
                  (select p.* from 
          (SELECT CLUSTER_CODE, Distribution_Channel,
          ROW_NUMBER() OVER(PARTITION BY CLUSTER_CODE ORDER BY Distribution_Channel) as rn
          from TM1_Table) p
          where p.rn =1) TM
                  on TM.CLUSTER_CODE = CUST.Cluster
          		left join TM1_Mapping_Table map_TM1
          		on TM.Distribution_Channel = map_TM1.Distribution_Channel
          	left join Customer_Mapping_Table map_Cust
		on CUST.DISTRIBUTION_CHANNEL = map_Cust.Distribution_Channel""")
		cust_DF.createOrReplaceTempView("Cust_Temp_Table")
		
		val channelDF = spark.sql("""select CUST.YCUSTMER,
        CASE WHEN CUST.DISTR_CHAN IN('KA','MT') 
         AND CUST.CUST_GROUP IN ('HYD','BLR','WME','WBC','WMC','BIP','RCC','GCC','CRF') THEN 'CNC'
         WHEN CUST.DISTR_CHAN IN('KA','MT') AND CUST.CUST_GROUP NOT IN ('HYD','BLR','WME','WBC','WMC','BIP','RCC','GCC','CRF') THEN 'Retail' ELSE NULL END CHANNEL
        from Customer_Table CUST
        """)
    channelDF.createOrReplaceTempView("channel_Table")
    
    val Result_DF= spark.sql("""select CUST.*,coalesce(CHAN.CHANNEL,CUST.Channel) as CHANNEL2
        from Cust_Temp_Table CUST
        left join channel_Table CHAN
        on CUST.CUSTOMER_KEY = CHAN.YCUSTMER""")
        
    val temp = Result_DF.drop("CHANNEL")
    
    val dimcustomerDF = temp.withColumnRenamed("CHANNEL2", "CHANNEL")
    
    dimcustomerDF.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(DestinationPath)

  }
}