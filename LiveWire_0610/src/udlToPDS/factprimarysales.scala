package udlToPDS

import org.apache.spark.sql.SparkSession
import java.util.HashMap
import landingtoudl.ResourceFunctions

object factprimarysales extends ResourceFunctions{
  def main(args: Array[String]): Unit = {
    
    val Dataset = "factprimarysales"
    val adlPath = args(0)
    val ConfigFile = args(1)
    
    val spark = SparkSession.builder().appName("factprimarysales").config("spark.executor.cores","5").getOrCreate()
    import spark.implicits._
    
    val paramHashMap:HashMap[String,String] = getConfiguration(ConfigFile, Dataset, spark)
    
    val Primary_Sales_TDP_Path = adlPath + paramHashMap.getOrDefault("Primary_Sales_TDP_Path", "NA")
    val Primary_Sales_Modified_Path = adlPath + paramHashMap.getOrDefault("Primary_Sales_Modified_Path", "NA")
    val Material_Path = adlPath + paramHashMap.getOrDefault("Material_Path", "NA")
    val Customer_Path = adlPath + paramHashMap.getOrDefault("Customer_Path", "NA")
    val factgstconversionfactor_Path = adlPath + paramHashMap.getOrDefault("factgstconversionfactor_Path", "NA")
    val Dimpricelisthierarchy_Path = adlPath + paramHashMap.getOrDefault("Dimpricelisthierarchy_Path", "NA")
    val Destination_Path = adlPath + paramHashMap.getOrDefault("Destination_Path", "NA")   
    
    
    val Primary_Sales_TDPDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Primary_Sales_TDP_Path)
    val Primary_Sales_ModifiedDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Primary_Sales_Modified_Path)
    val MaterialDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Material_Path)
    val CustomerDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(Customer_Path)
    val factgstconversionfactorDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(factgstconversionfactor_Path)
    val DimpricelisthierarchyDF =  spark.read.option("header", "true").csv(Dimpricelisthierarchy_Path)

    Primary_Sales_TDPDF.createOrReplaceTempView("Primary_Sales_TDP")
    Primary_Sales_ModifiedDF.createOrReplaceTempView("Primary_Sales_Modified")
    MaterialDF.createOrReplaceTempView("Material")
    CustomerDF.createOrReplaceTempView("Customer")
    factgstconversionfactorDF.createOrReplaceTempView("factgstconversionfactor")
    DimpricelisthierarchyDF.createOrReplaceTempView("Dimpricelisthierarchy")

    /*
    val Primary_SalesDF = spark.sql("""
    		select YINV_TYP,YSLTYP,0COMP_CODE,YCUSTMER,YSHIPTO,YMATERL,0INV_QTY,YINV_QTY4,YQTY_CLD,YGSV,YGOCVL,YPOVGSV,YBSPACK,Y10DAYPRD
    		from Primary_Sales_TDP
    		union all
    		select YINV_TYP,YSLTYP,0COMP_CODE,CUSTMER as YCUSTMER,YSHIPTO,YMATERL,0INV_QTY,YINV_QTY4,YQTY_CLD,YGSV,YGOCVL,YPOVGSV,YBSPACK,Y10DAYPRD
    		from Primary_Sales_Modified
    		""")
    */
    val Primary_SalesDF = spark.sql("""
    		select 0CALMONTH,YINV_TYP,YSLTYP,0COMP_CODE,YCUSTMER,YSHIPTO,YMATERL,0INV_QTY,YINV_QTY4,YQTY_CLD,YGSV,YGOCVL,YPOVGSV,YBSPACK,Y10DAYPRD
    		from Primary_Sales_Modified
    		union all
    		select 0CALMONTH,YINV_TYP,YSLTYP,0COMP_CODE,CUSTOMER as YCUSTMER,YSHIPTO,MATERIAL as YMATERL,0INV_QTY,YINV_QTY4,YQTY_CLD,YGSV,YGOCVL,YPOVGSV,YBSPACK,Y10DAYPRD
    		from Primary_Sales_TDP
    		""")
    		Primary_SalesDF.createOrReplaceTempView("Primary_Sales_Union")
    		
    		/*
    		val factprimarysales_tempDF = spark.sql("""
            select YINV_TYP,
    				YSLTYP,
    				0COMP_CODE,
    				LPAD(YCUSTMER,10,0) as YCUSTMER,
    				YSHIPTO,
    				YMATERL,
    				0INV_QTY as QTY_PCS,
    				YINV_QTY4 as QTY_KGS,
    				YQTY_CLD as QTY_CASES,
    				YGSV
    				from Primary_Sales_Union
    				""")
    		
    		factprimarysales_tempDF.createOrReplaceTempView("factprimarysales_temp")*/
    		

    		val Res_DF = spark.sql("""select concat(0CALMONTH,"01") as time_key,
            YINV_TYP as INVC_TYPE, 
    				YSLTYP as SLS_TYPE,
    				`0COMP_CODE` as CO_ID,
    				LPAD(PS.YCUSTMER,10,0) as CUSTOMER,
    				YSHIPTO as SHIP_TO,
    				PS.YMATERL as MATERIAL,
    				0INV_QTY as QTY_PCS,
    				YINV_QTY4 as QTY_KGS,
    				YQTY_CLD as QTY_CASES,
    				YGSV as GSV,
            Mat.YMATERL MatYMATERL,
    				case 
    				  when Mat.YSTDBUSG  IN ('BEVG','DETS','FOODS','PP','ICD','UFS','WATER')
    				      and Cus.YCUSCHN  IN ('GACC','WMCC','CRFR','METRO','WRMC','RECC','BIPL') 
    				      and  Cus.YCUSCHNL  IN ('MT','KA') 
    				  Then "CNC"
    				  when Mat.YSTDBUSG IN ('BEVG','DETS','FOODS','PP','ICD','UFS','WATER')
    				    and  Cus.YCUSCHN NOT IN ('GACC','WMCC','CRFR','METRO','WRMC','RECC','BIPL') 
    				    and  Cus.YCUSCHNL  IN ('MT','KA')
    				  Then "MT_Retail"
    				  when Mat.YSTDBUSG IN  ('DETS','PP','BEVG','FOODS')
    				    and Cus.YCUSCHNL='CD'
    				    Then "CSD"
    				  when Mat.YSTDBUSG IN  ('DETS','PP','BEVG','FOODS')
    				    and Cus.YCUSCHNL='GT'
    				  Then "GT"
    				  when Mat.YSTDBUSG IN  ('DETS','PP','BEVG','FOODS')
    				    and Cus.YCUSCHNL='RD'
    				  Then "RD"
    				  When Mat.YSTDBUSG IN  ('DETS','PP','BEVG','FOODS')
    				    and Cus.YCUSCHNL='OH'
    				  Then "OOH"
    				  else NULL
    				end as DISTRIBUTION_CHANNEL,
          YGSV+YGOCVL+YPOVGSV as GSV_WITHOUT_POGO
    			from Primary_Sales_Union PS 
    			join Customer Cus
    			on PS.YCUSTMER = Cus.YCUSTMER
    			join Material Mat
    			on PS.YMATERL = Mat.YMATERL
    			Where 
    				Cus.YBRANCH  IN ('B001','B002','B003','B004','B010')
    				and Mat.YSTDSCAT  not  IN ('SG_BB03206','IV','TV')---> Same
    				and  Mat.YSTDBRND  <> 'BR_MTKD'
    				and  Mat.YSTDBUS  IN ('FOOD','HPC')
    				AND  Mat.YPRICELST IN (SELECT Pricelist_code FROM Dimpricelisthierarchy   WHERE Price_list_group  IN ('DETS','FNB','PP','HUL3'))
    				AND  Mat.YMATERL not in ('11630','12738','12739','12740','13019','13050','14009','14011','14105','16047','16052','16056','16069','16076','16079','16202','16204','16258','16259','16285','16286','75364','75365','80109','80191','80432','80433','80434','80435')
    				and  PS.0COMP_CODE  IN ('K','H')
    				and  PS.YSLTYP  IN ('C','N')
    				and  PS.YINV_TYP IN ('A','C','D','DM','N','S')""")
    				
    		val gst = spark.sql("""select Material.YMATERL,
    				CASE WHEN `0CALMONTH`<'201701' and YBUSNSS in ('FOODS','REFRESHMENTS') 
    				THEN  YGSV*1.1
    				WHEN `0CALMONTH`<'201701' and YBUSNSS not in ('FOODS','REFRESHMENTS') 
    				THEN YGSV*1.1825
    				ELSE
    				factgstconversionfactor.Conv_Rate*PSU.YGSV 
    				end as GSV_TUR
    				from 
    				Primary_Sales_Union PSU
    				join Material 
    				on PSU.YMATERL = Material.YMATERL
    				join factgstconversionfactor
    				on factgstconversionfactor.Sales_Category = Material.YSTDCATY
    				and factgstconversionfactor.Sales_Brand=Material.YSTDBRND
    				Where 
    				PSU.0CALMONTH between Substring(factgstconversionfactor.Start_Date,0,4) and Substring(factgstconversionfactor.End_Date,0,4) 
    				and PSU.0CALMONTH>='201701'""")
    				
    				gst.createOrReplaceTempView("gst_table")
    				Res_DF.createOrReplaceTempView("res_table")
    				
    		val Result_DF =	spark.sql("select res.*,GSV_TUR from res_table res left join gst_table gst on res.MatYMATERL =gst.YMATERL")
    		val factprimarysalesDF = Result_DF.drop("MatYMATERL")
    		
    		factprimarysalesDF.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination_Path)
    		// ResOp.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(DestinationPath)
    				
    		
    				
    				
   
    
  }
}