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

object dimTime extends ResourceFunctions {
	def main(args: Array[String]): Unit = {

			//val WorkingDayCalendar_Path = "C:/Users/mandeep.l.kaur/Desktop/dimTime/UDL/IN_WORKING_DAY_CALENDAR.csv"
			//val TDP_Calendar_Path = "C:/Users/mandeep.l.kaur/Desktop/dimTime/UDL/IN_TDP_CALENDAR.csv"

			// val WorkingDayCalendar_Path = "adl://bienodad57179stgadls.azuredatalakestore.net/Unilever/UniversalDataLake/InternalSources/FusionBW/OpenHub/WorkingDayCalendar/India/IN_WORKING_DAY_CALENDAR.csv"
			//val TDP_Calendar_Path = "adl://bienodad57179stgadls.azuredatalakestore.net/Unilever/UniversalDataLake/InternalSources/FusionBW/OpenHub/TDPCalendar/India/IN_TDP_CALENDAR.csv"

			// val DestinationPath = "adl://bienodad57179stgadls.azuredatalakestore.net/Unilever/ProductDataStores/LiveWire/India/dimTime"

			//val spark = SparkSession.builder().appName("dimTime").master("local[*]").getOrCreate()

			var sb = new StringBuffer
					try {
						val DataSet = "dimTime"
								val adlPath = args(0)
								val ConfigFile = args(1)
								val spark = SparkSession.builder().appName("dimTime").getOrCreate()
								import spark.implicits._

								val paramHashMap: HashMap[String, String] = getConfiguration(ConfigFile, DataSet, spark)
								sb.append("paramhashMap: " + paramHashMap + "\n")

								val WorkingDayCalendar_Path = adlPath + paramHashMap.getOrDefault("WorkingDayCalendar_Path", "NA")
								sb.append("WorkingDayCalendar_Path: " + WorkingDayCalendar_Path + "\n")
								val TDP_Calendar_Path = adlPath + paramHashMap.getOrDefault("TDP_Calendar_Path", "NA")
								sb.append("TDP_Calendar_Path: " + TDP_Calendar_Path + "\n")
								val Destination = adlPath + paramHashMap.getOrDefault("Destination", "NA")
								sb.append("Destination: " + Destination + "\n")

								val WorkingDayCalendarDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(WorkingDayCalendar_Path)
								WorkingDayCalendarDF.createOrReplaceTempView("WorkingDayCalendar_Table")
								val TDP_CalendarDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load(TDP_Calendar_Path)
								TDP_CalendarDF.createOrReplaceTempView("TDP_Calendar_Table")

								val Format_ddMMMyyyy = new SimpleDateFormat("ddMMMyyyy")
								val Format_yyyy_MM_dd = new SimpleDateFormat("yyyy-MM-dd")
								val Format_yyyyMMdd = new SimpleDateFormat("yyyyMMdd")
								val Format_MMMyyyy = new SimpleDateFormat("MMM yyyy")
								val Format_yyyyMMMdd = new SimpleDateFormat("yyyyMMMdd")

								//Start of MAT Start UDF. It calculates the Moving Annual Total base on Date column

								val MAT_Start = ((date: String) => {
									var op = ""
											try {
												val currentDate = Format_ddMMMyyyy.parse(date)
														currentDate.setDate(1)

														val currentCal = Calendar.getInstance()
														currentCal.setTime(currentDate)
														currentCal.add(Calendar.MONTH, -13)

														op = Format_ddMMMyyyy.format(currentCal.getTime)
											} catch {
											case t: Throwable => op = ""
											}
									op
								})

								val MAT_Start_UDF = spark.udf.register("MAT_Start", MAT_Start)

								//End of MAT Start UDF

								//Start of getMinDate UDF. It returns first day of the month

								def getMinDate(InpDate: Date): Date = {
										try {
											InpDate.setDate(1)
										} catch {
										case t: Throwable => t.printStackTrace() // TODO: handle error
										}
										InpDate
								}
								//End of getMinDate UDF

								//Start of getMaxDate UDF. It returns last day of the month
								def getMaxDate(InpDate: Date): Date = {
										val gc = new GregorianCalendar
												val MinDate = getMinDate(InpDate)
												gc.set(MinDate.getYear + 1900, MinDate.getMonth, MinDate.getDate)
												gc.add(Calendar.MONTH, 1)
												gc.add(Calendar.DATE, -1)
												gc.getTime
								}
								//End of getMaxDate UDF

								//Start of L3M Start UDF. It calculates last 3 months based on date column

								val L3M_Start = ((date: String) => {
									var op = ""
											try {
												val currentDate = Format_ddMMMyyyy.parse(date)
														currentDate.setDate(1)
														val currentCal = Calendar.getInstance();
												currentCal.setTime(currentDate)
												currentCal.add(Calendar.MONTH, -3);
												op = Format_ddMMMyyyy.format(currentCal.getTime)
											} catch {
											case t: Throwable => op = ""
											}
									op
								})

										val L3M_Start_UDF = spark.udf.register("L3M_Start", L3M_Start)

										//End of L3M Start UDF

										//Start of YA_L3M_START UDF

										val YA_L3M_Start = ((date: String) => {
											var op = ""
													try {
														val currentDate = Format_ddMMMyyyy.parse(date)
																currentDate.setDate(1)
																val currentCal = Calendar.getInstance();
														currentCal.setTime(currentDate)
														currentCal.add(Calendar.MONTH, -15);
														op = Format_ddMMMyyyy.format(currentCal.getTime)
													} catch {
													case t: Throwable => op = ""
													}
											op
										})

										val YA_L3M_Start_UDF = spark.udf.register("YA_L3M_Start", YA_L3M_Start)

										//End of YA_L3M_START UDF

										//Start of YA_MAT_Start UDF

										val YA_MAT_Start = ((date: String) => {
											var op = ""
													try {
														val currentDate = Format_ddMMMyyyy.parse(date)
																currentDate.setDate(1)

																val currentCal = Calendar.getInstance()
																currentCal.setTime(currentDate)
																currentCal.add(Calendar.MONTH, -25)
																op = Format_ddMMMyyyy.format(currentCal.getTime)
													} catch {
													case t: Throwable => op = ""
													}
											op
										})

										val YA_MAT_Start_UDF = spark.udf.register("YA_MAT_Start", YA_MAT_Start)

										//End of YA_MAT_Start UDF

										//Start of YA_MAT_END UDF

										val YA_MAT_END = ((date: String) => {

											var op = ""
													try {
														val currentDate = Format_ddMMMyyyy.parse(date)
																val eom = getMaxDate(currentDate)
																val currentCal6 = Calendar.getInstance();
														currentCal6.setTime(eom)
														currentCal6.add(Calendar.MONTH, -13);
														op = Format_ddMMMyyyy.format(currentCal6.getTime)
													} catch {
													case t: Throwable => op = ""
													}
											op
										})

										val YA_MAT_END_UDF = spark.udf.register("YA_MAT_END", YA_MAT_END)

										//End of YA_MAT_END UDF

										//Evaluates first day of the Year

										val firstYear = ((date: String) => {
											var op = ""
													try {
														val currentDate = Format_ddMMMyyyy.parse(date)
																val yearnew1 = currentDate
																yearnew1.setMonth(0)
																yearnew1.setDate(1)
																op = Format_ddMMMyyyy.format(yearnew1.getTime)
													} catch {
													case t: Throwable => op = ""
													}
											op
										})

										val firstyear_UDF = spark.udf.register("firstYear", firstYear)

										//Evaluates first day of the Month

										val firstMonth = ((date: String) => {

											var op = ""
													try {
														val currentDate = Format_ddMMMyyyy.parse(date)
																val yearnew1 = currentDate
																yearnew1.setDate(1)
																op = Format_ddMMMyyyy.format(yearnew1.getTime())
																//op = yearStart.toString
													} catch {
													case t: Throwable => op = ""
													}
											op
										})

										val firstMonth_UDF = spark.udf.register("firstMonth", firstMonth)

										//Evaluates first day of the Quarter

										val firstQtr = ((date: String) => {
											var op = ""
													try {
														val currentDate = Format_ddMMMyyyy.parse(date)
																var mon = currentDate.getMonth
																if (mon == 1 | mon == 2)
																	mon = 0
																	else if (mon == 4 | mon == 5)
																		mon = 3
																		else if (mon == 7 | mon == 8)
																			mon = 6
																			else if (mon == 10 | mon == 11)
																				mon = 9

																				currentDate.setMonth(mon)
																				currentDate.setDate(1)
																				op = Format_ddMMMyyyy.format(currentDate.getTime)
													} catch {
													case t: Throwable => op = ""
													}
											op
										})

										val firstQuarter_UDF = spark.udf.register("firstQtr", firstQtr)

										//Start of YA_L3M_END UDF
										val YA_L3M_END = ((date: String) => {
											var op = ""
													try {
														val currentDate = Format_ddMMMyyyy.parse(date)
																val mle = getMaxDate(currentDate)

																val currentCal = Calendar.getInstance();
														currentCal.setTime(mle)
														currentCal.add(Calendar.MONTH, -13);
														op = Format_ddMMMyyyy.format(currentCal.getTime)
													} catch {
													case t: Throwable => t.printStackTrace() // TODO: handle error
													}
											op
										})
										val YA_L3M_END_UDF = spark.udf.register("YA_MAT_L3M_END", YA_L3M_END)

										//End of YA_L3M_END UDF

										//Start of MAT_L3M_END UDF

										val MAT_L3M_END = ((date: String) => {
											var op = ""
													try {
														val currentDate = Format_ddMMMyyyy.parse(date)
																val yml = getMaxDate(currentDate)

																val currentCal = Calendar.getInstance();
														currentCal.setTime(yml)
														currentCal.add(Calendar.MONTH, -1);
														op = Format_ddMMMyyyy.format(currentCal.getTime)
													} catch {
													case t: Throwable => op = ""
													}
											op
										})

										val MAT_L3M_END_UDF = spark.udf.register("MAT_L3M_END", MAT_L3M_END)
										//End of MAT_L3M_END UDF

										//Start of MOCStart UDF
										val MOCStart = ((date: String) => {
											var op = ""
													try {
														val yearnew = Format_ddMMMyyyy.parse(date)
																val dt = yearnew.getDate()
																val newdate = yearnew
																var mon = yearnew.getMonth()
																if (dt < 21)
																	mon = mon - 1

																	newdate.setMonth(mon)
																	newdate.setDate(21)

																	op = Format_ddMMMyyyy.format(newdate)
													} catch {
													case t: Throwable => op = ""
													}
											op
										})
										val MOC_Start_Date = spark.udf.register("MOCStart", MOCStart)
										//END of MOCStart UDF

										//Start of TimeKey UDF. It changes the format of Date column from ddMMMyyyy to yyyyMMdd format

										val TimeKey = ((date: String) => {
											var op = date
													// val outputFormat = new SimpleDateFormat("yyyyMMdd")
													try {
														val FormattedDate = Format_yyyyMMdd.format(Format_ddMMMyyyy.parse(date))
																op = FormattedDate
													} catch {
													case t: Throwable => op = ""
													}
											op
										})

										val DateConvert = spark.udf.register("TimeKey", TimeKey)
										//END of TimeKey UDF

										//Start of TDP UDF. It changes the format of TDP_START and TDP_END columns from yyyyMMMdd to ddMMMyyyy format

										val TDP = ((date: String) => {
											var op = date
													// val outputFormat = new SimpleDateFormat("yyyyMMdd")
													try {
														val FormattedDate = Format_ddMMMyyyy.format(Format_yyyyMMMdd.parse(date))
																op = FormattedDate
													} catch {
													case t: Throwable => op = ""
													}
											op
										})

										val TDPDateConvert = spark.udf.register("TDP", TDP)
										//END of TimeKey UDF

										//Start of QTR_END_NAME UDF

										val QTR_END_NAME = ((date: String) => {
											var op = ""
													try {
														val yearnew = Format_ddMMMyyyy.parse(date)
																var mon = yearnew.getMonth
																if (mon == 1 | mon == 2)
																	mon = 0
																	else if (mon == 4 | mon == 5)
																		mon = 3
																		else if (mon == 7 | mon == 8)
																			mon = 6
																			else if (mon == 10 | mon == 11)
																				mon = 9

																				yearnew.setMonth(mon + 2)
																				yearnew.setDate(1)
																				val QtrStart = Format_MMMyyyy.format(yearnew)
																				op = QtrStart.toString
													} catch {
													case t: Throwable => op = ""
													}
											op
										})

										val QTR_END_NAME_UDF = spark.udf.register("QTR_END_NAME", QTR_END_NAME)

										//Start of Date_Calendar UDF. It takes ddMMMyyyy in String format and Converts it into date yyyy-MM-dd format.

										val Date_Calendar = ((date: String) => {

											var op = ""
													try {
														//op = Format_yyyy_MM_dd.format(Format_ddMMMyyyy.parse(date))
														op = Format_ddMMMyyyy.format(Format_ddMMMyyyy.parse(date))
													} catch {
													case t: Throwable => op = ""
													}
											op
										})
										val Date_Calendar_UDF = spark.udf.register("Date_Calendar", Date_Calendar)
										//END of Date_Calendar UDF

										//Start of LAST_30_DAYS UDF

										val LAST_30_DAYS = ((date: String) => {
											var op = ""
													try {
														val currentDate = Format_ddMMMyyyy.parse(date)
																val currentCal = Calendar.getInstance();
														currentCal.setTime(currentDate)
														currentCal.add(Calendar.DATE, -30);
														op = Format_ddMMMyyyy.format(currentCal.getTime)
													} catch {
													case t: Throwable => op = ""
													}
											op
										})

										val LAST_30_DAYS_UDF = spark.udf.register("LAST_30_DAYS", LAST_30_DAYS)
										//END of LAST_30_DAYS UDF

										//Evaluates YEAR_END

										val YEAR_END = ((date: String) => {
											var op = ""
													try {
														val yearnew = Format_ddMMMyyyy.parse(date)
																val year = yearnew.getYear - 1
																val yearnew1 = yearnew
																yearnew1.setMonth(11)
																yearnew1.setDate(31)
																yearnew1.setYear(year)

																op = Format_ddMMMyyyy.format(yearnew1)

													} catch {
													case t: Throwable => op = ""
													}
											op
										})

										val YEAR_END_UDF = spark.udf.register("YEAR_END", YEAR_END)
										//END of YEAR_END UDF

										//START of First day of last month UDF

										val first_day_last_month = ((date: String) => {
											var op = ""
													try {
														val currentDate = Format_ddMMMyyyy.parse(date)
																var mon = currentDate.getMonth
																currentDate.setMonth(mon - 1)
																currentDate.setDate(1)
																op = Format_ddMMMyyyy.format(currentDate)
													} catch {
													case t: Throwable => op = ""
													}
											op
										})

										val firstdaylastmonth_UDF = spark.udf.register("first_day_last_month", first_day_last_month)
										//End of First day of last month UDF

										//START of FIRST_DAY_LAST_YEAR UDF

										val first_day_last_year = ((date: String) => {

											var op = ""
													try {
														val currentDate = Format_ddMMMyyyy.parse(date)
																var mon = currentDate.getMonth
																currentDate.setMonth(0)
																currentDate.setDate(1)
																var year = currentDate.getYear
																currentDate.setYear(year - 1)
																op = Format_ddMMMyyyy.format(currentDate)
													} catch {
													case t: Throwable => op = ""
													}
											op
										})
										val firstdaylastyear_UDF = spark.udf.register("first_day_last_year", first_day_last_year)
										//END of FIRST_DAY_LAST_YEAR UDF

										//START OF FIRST_DAY_LAST_QUARTER

										val first_day_last_quarter = ((date: String) => {

											var op = ""
													try {
														val currentDate = Format_ddMMMyyyy.parse(date)
																// val yearnew1 = inputFormat.parse(date)
																var mon = currentDate.getMonth
																val yearnew1 = currentDate
																var year = currentDate.getYear
																// yearnew.setYear(year-1)
																//   yearnew1.setDate(1)
																if (mon == 0 | mon == 1 | mon == 2) {
																	mon = 9
																			currentDate.setYear(year - 1)
																} else if (mon == 3 | mon == 4 | mon == 5) {
																	mon = 0
																} else if (mon == 6 | mon == 7 | mon == 8) {
																	mon = 3
																} else if (mon == 9 | mon == 10 | mon == 11) {
																	mon = 6
																}
														currentDate.setMonth(mon)
														yearnew1.setDate(1)
														// yearnew.setDate(yearnew1)
														// var year = yearnew.getYear
														//yearnew.setYear(year-1)
														op = Format_ddMMMyyyy.format(currentDate)
													} catch {
													case t: Throwable => op = ""
													}
											op
										})

										val firstdaylastquarter_UDF = spark.udf.register("first_day_last_quarter", first_day_last_quarter)
										//END of FIRST_DAY_LAST_QUARTER

										//START of LAST_DAY_LAST_QUARTER
										val last_day_last_quarter = ((date: String) => {
											var op = ""
													try {
														val currentDate = Format_ddMMMyyyy.parse(date)

																var mon = currentDate.getMonth
																var newdate = currentDate.getDate
																var year = currentDate.getYear
																if (mon == 0 | mon == 1 | mon == 2) {
																	mon = 11
																			newdate = 31
																			currentDate.setYear(year - 1)
																} else if (mon == 3 | mon == 4 | mon == 5) {
																	mon = 2
																			newdate = 31
																} else if (mon == 6 | mon == 7 | mon == 8) {
																	mon = 5
																			newdate = 30
																} else if (mon == 9 | mon == 10 | mon == 11) {
																	mon = 8
																			newdate = 30
																}

														currentDate.setMonth(mon)
														currentDate.setDate(newdate)
														op = Format_ddMMMyyyy.format(currentDate)
													} catch {
													case t: Throwable => op = ""
													}
											op
										})

										val lastdaylastquarter_UDF = spark.udf.register("last_day_last_quarter", last_day_last_quarter)
										//END of LAST_DAY_LAST_QUARTER

										//Spark SQL query to evaluate TDP_START,TDP_END and other required columns

										val Res_Df_temp = spark.sql("""SELECT A.*, B.FROM_DATE TDP_START,B.TO_DATE TDP_END,concat("Q",substring(QTR,5,1)) as CALENDAR_QUARTER,
												CASE WHEN substring(A.Period, 6,2)='01' THEN 'January'
												WHEN substring(A.Period, 6,2)='02' THEN 'February'
												WHEN substring(A.Period, 6,2)='03' THEN 'March'
												WHEN substring(A.Period, 6,2)='04' THEN 'April'
												WHEN substring(A.Period, 6,2)='05' THEN 'May'
												WHEN substring(A.Period, 6,2)='06' THEN 'June'
												WHEN substring(A.Period, 6,2)='07' THEN 'July'
												WHEN substring(A.Period, 6,2)='08' THEN 'August'
												WHEN substring(A.Period, 6,2)='09' THEN 'September'
												WHEN substring(A.Period, 6,2)='10' THEN 'October'
												WHEN substring(A.Period, 6,2)='11' THEN 'November'
												WHEN substring(A.Period, 6,2)='12' THEN 'December'
												END as MONTH_NAME
												FROM WorkingDayCalendar_Table A
												LEFT JOIN TDP_Calendar_Table B  ON concat(A.YEAR,lpad(A.TDP_NO,2,0))=B.MOC_TDP_CODE""")

										Res_Df_temp.createOrReplaceTempView("Res_Df_temp_Table")
										val df2 = spark.sql("Select * from Res_Df_temp_Table")

										// df2.show(50)

										// val Res_Df_temp1 = spark.sql("""SELECT A.*,concat(MONTH_NAME,' ',YEAR) as MONTH_YEAR_NAME,concat(CALENDAR_QUARTER,' ',YEAR)  as QTR_YEAR_NAME
										// from Res_Df_temp_Table A""")

										val Res_Df_temp1 = spark.sql("""SELECT A.*,concat(SUBSTRING(MONTH_NAME,1,3),' ',YEAR) as MONTH_YEAR_NAME,concat(CALENDAR_QUARTER,' ',YEAR)  as QTR_YEAR_NAME 
												from Res_Df_temp_Table A""")

										Res_Df_temp1.createOrReplaceTempView("Res_Df_temp_Table1")
										val df3 = spark.sql("Select * from Res_Df_temp_Table1")

										// df3.show(20)

										val InpDF1 = Res_Df_temp1.withColumn("MAT_START", MAT_Start_UDF(WorkingDayCalendarDF("DATE")))
										.withColumn("CALENDAR_WEEK", lit(""))
										.withColumn("#DAYS_MONTH", lit(""))
										.withColumn("#DAYS_QURTER", lit(""))
										.withColumn("#DAYS_YEAR", lit(""))
										.withColumn("COUNTRY_WEEK", lit(""))
										.withColumn("COUNTRY_MONTH", lit(""))
										.withColumn("FISCAL_YEAR", lit(""))
										.withColumn("FISCAL_YEAR", lit(""))
										.withColumn("FISCAL_PERIOD", lit(""))
										// .withColumn("MOC_START_DATE", MOC_Start_Date(col("DATE")))
										.withColumn("MOC_START_DATE", MOC_Start_Date(WorkingDayCalendarDF("DATE")))
										.withColumn("L3M_START", L3M_Start_UDF(WorkingDayCalendarDF("DATE")))
										.withColumn("YA_L3M_START", YA_L3M_Start_UDF(WorkingDayCalendarDF("DATE")))
										.withColumn("YA_MAT_START", YA_MAT_Start_UDF(WorkingDayCalendarDF("DATE")))
										.withColumn("YA_MAT_END", YA_MAT_END_UDF(WorkingDayCalendarDF("DATE")))
										.withColumn("YA_L3M_END", YA_L3M_END_UDF(WorkingDayCalendarDF("DATE")))
										.withColumn("MAT_L3M_END", MAT_L3M_END_UDF(WorkingDayCalendarDF("DATE")))
										.withColumn("FIRST_DAY_YEAR", firstyear_UDF(WorkingDayCalendarDF("DATE")))
										.withColumn("FIRST_DAY_MONTH", firstMonth_UDF(WorkingDayCalendarDF("DATE")))
										.withColumn("TIME_KEY", DateConvert(WorkingDayCalendarDF("DATE")))
										.withColumn("CALENDAR_MONTH", substring(DateConvert(WorkingDayCalendarDF("DATE")), 0, 6))
										.withColumn("LAST_30_DAY", LAST_30_DAYS_UDF(WorkingDayCalendarDF("DATE")))
										.withColumn("YEAR_END", YEAR_END_UDF(WorkingDayCalendarDF("DATE")))
										.withColumn("TDP_START", TDPDateConvert(Res_Df_temp("TDP_START")))
										.withColumn("TDP_END", TDPDateConvert(Res_Df_temp("TDP_END")))

										.withColumn("FIRST_DAY_QUARTER", firstQuarter_UDF(WorkingDayCalendarDF("DATE")))
										.withColumn("QTR_END_NAME", QTR_END_NAME_UDF(WorkingDayCalendarDF("DATE")))
										.withColumn("FIRST_DAY_LAST_MONTH", firstdaylastmonth_UDF(WorkingDayCalendarDF("DATE")))
										.withColumn("FIRST_DAY_LAST_YEAR", firstdaylastyear_UDF(WorkingDayCalendarDF("DATE")))
										.withColumn("FIRST_DAY_LAST_QUARTER", firstdaylastquarter_UDF(WorkingDayCalendarDF("DATE")))
										.withColumn("LAST_DAY_LAST_QUARTER", lastdaylastquarter_UDF(WorkingDayCalendarDF("DATE")))
										.withColumn("DATE", Date_Calendar_UDF(WorkingDayCalendarDF("DATE")))

										.select(
												col("TIME_KEY").as("TIME_KEY"),
												col("DATE").as("DATE"),
												col("CALENDAR_QUARTER"),
												col("CALENDAR_WEEK"),
												col("CALENDAR_MONTH").as("CALENDAR_MONTH"),
												col("YEAR"),
												col("MAT_START"),
												col("L3M_START"),
												col("YA_L3M_START"),
												col("YA_L3M_END"),
												col("YA_MAT_START"),
												col("YA_MAT_END"),
												col("MAT_L3M_END"),
												col("COUNTRY_WEEK"),
												col("COUNTRY_MONTH"),
												col("TDP_No") as ("TDP"),
												col("WORKING_DAYS"),
												col("FIRST_DAY_MONTH"),
												col("FISCAL_YEAR"),
												col("FISCAL_PERIOD"),
												col("TDP_START"),
												col("TDP_END"),
												col("FIRST_DAY_QUARTER"),
												col("FIRST_DAY_YEAR"),
												col("MOC_START_DATE"),
												col("DATE").as("MOC_END_DATE"),
												col("#DAYS_MONTH"),
												col("#DAYS_QURTER"),
												col("#DAYS_YEAR"),
												col("MONTH_NAME"),
												col("QTR_END_NAME"),
												col("MONTH_YEAR_NAME"),
												col("QTR_YEAR_NAME"),
												col("LAST_30_DAY"),
												col("YEAR_END"),
												col("FIRST_DAY_LAST_MONTH"),
												col("FIRST_DAY_LAST_YEAR"),
												col("FIRST_DAY_LAST_QUARTER"),
												col("LAST_DAY_LAST_QUARTER"))

										//InpDF1.show()
										// InpDF1.createOrReplaceTempView("InpDF1DF_Table")

										// -- Build for creating a destination path, if not available -->
										val hadoopConf = new org.apache.hadoop.conf.Configuration()
										val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(Destination), hadoopConf)

										if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(Destination))) {
											sb.append("Creating a new Destination dir : " + Destination + "\n")
											hdfs.mkdirs(new org.apache.hadoop.fs.Path(Destination))
										}

								// <-- Build complete for creating a destination path.

								InpDF1.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(Destination)

					} catch {
					case t: Throwable => t.printStackTrace() // TODO: handle error
					}
			println(sb.toString())

	}
} 