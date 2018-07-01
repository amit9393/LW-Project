package landingtoudl

import scala.collection.immutable.HashMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

class Transaction extends ResourceFunctions {
  def processTransaction(InpParam: java.util.HashMap[String, String], spark: SparkSession, hadoopConf: Configuration, hdfs: FileSystem): StringBuffer = {

    var sb: StringBuffer = new StringBuffer("Inside processTransaction method\n")

    try {
      val StartTime = currentTimestamp()
      sb.append("StartTime is : " + StartTime + "\n")

      val Adl_Path = InpParam.getOrDefault("Adl_Path", "NA")
      val LandingPath = Adl_Path + InpParam.getOrDefault("LandingPath", "NA")
      val DataSet_Name = InpParam.getOrDefault("DataSet", "NA")
      val UDLpath = Adl_Path + InpParam.getOrDefault("UDLpath", "NA")
      val LandingFileName = InpParam.getOrDefault("LandingFileName", "NA")
      val UDLFileName = InpParam.getOrDefault("UDLFileName", "NA")
      val ErrorPath = Adl_Path + InpParam.getOrDefault("ErrorPath", "NA")
      val schema = InpParam.getOrDefault("schema", "NA")
      val header = InpParam.getOrDefault("header", "NA")
      val delimiter = InpParam.getOrDefault("delimiter", "NA")
      val NotNullColumn = InpParam.getOrDefault("NotNullColumn", "NA")
      val SchemaValidationColumn = InpParam.getOrDefault("SchemaValidationColumn", "NA")
      val duplicateRow = InpParam.getOrDefault("duplicateRow", "NA")
      var mode = InpParam.getOrDefault("mode", "NA")
      val SplitFileOnDate = InpParam.getOrDefault("SplitFileOnDate", "NA")
      val SplitDateColumn = InpParam.getOrDefault("SplitDateColumn", "NA")
      val SingleFile = InpParam.getOrDefault("SingleFile", "NA")
      val Ingestion_type = InpParam.getOrDefault("Ingestion_type", "NA")
      val DateFolder = InpParam.getOrDefault("DateFolder", "NA")
      val ReferentialString = InpParam.getOrDefault("ReferentialString", "NA")
      val RejectReferentialData = InpParam.getOrDefault("RejectReferentialData", "NA")
      val mappingPrimaryKey = InpParam.getOrDefault("mappingPrimaryKey", "NA")
      val mappingFilePath = Adl_Path+InpParam.getOrDefault("mappingFilePath", "NA")
      val HarmonizeDateColumn = InpParam.getOrDefault("HarmonizeDateColumn", "NA")
      val HarmonizedDateFormat = InpParam.getOrDefault("HarmonizedDateFormat", "ddMMMyyyy")
      val rejectDateHarmonize = InpParam.getOrDefault("rejectDateHarmonize", "NA")
      val rejectDuplicateData = InpParam.getOrDefault("rejectDuplicateData", "NA")
      val rejectLengthData = InpParam.getOrDefault("rejectLengthData", "NA")
      val rejectDecimalData = InpParam.getOrDefault("rejectDecimalData", "NA")
      val rejectDateData = InpParam.getOrDefault("rejectDateData", "NA")
      val rejectInteger = InpParam.getOrDefault("rejectInteger", "NA")
      val lengthOffset = InpParam.getOrDefault("lengthOffset", "0")
      
      //val DateFolder = CurrentDate()

      var Res_ErrorPath = ""
      var LandingFilePath = ""

      /*if (Ingestion_type.toUpperCase().equals("FULL_LOAD")) {
        sb.append("Ingestion type is FULL_LOAD hence deleting the UDL dir : " + UDLpath + "\n")
        hdfs.delete(new org.apache.hadoop.fs.Path(UDLpath))
      }*/

      if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(UDLpath))) {
        sb.append("Creating a new UDL dir : " + UDLpath + "\n")
        hdfs.mkdirs(new org.apache.hadoop.fs.Path(UDLpath))
      }

      var FileFormat = ""

      var filepath = Array[String]()

      if (!DateFolder.equals("NA")) {
        Res_ErrorPath = ErrorPath + "/" + DateFolder
        sb.append("Error will be written in the folder : " + Res_ErrorPath + "\n")

        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(Res_ErrorPath))) {
          sb.append("Creating a Error dir : " + Res_ErrorPath + "\n")
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(Res_ErrorPath))
        }

        sb.append("Listing the files in dir : " + LandingPath + "/" + DateFolder + "\n")
        val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(Adl_Path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(LandingPath + "/" + DateFolder + "/" + LandingFileName + "*".replaceAll(Adl_Path, "")))
        sb.append("listStatus length: " +listStatus.length + "\n")
        filepath = listStatus.map(_.getPath.toString())
        sb.append("filepath length: " +filepath.length + "\n")
      } else {
        Res_ErrorPath = ErrorPath+"/"+CurrentDate()
        sb.append("Error will be written in the folder : " + Res_ErrorPath + "\n")

        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(Res_ErrorPath))) {
          sb.append("Creating a Error dir : " + Res_ErrorPath + "\n")
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(Res_ErrorPath))
        }
        sb.append("Listing the files in dir : " + LandingPath + "\n")
        val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(Adl_Path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(LandingPath + "/" + LandingFileName + "*".replaceAll(Adl_Path, "")))

        filepath = listStatus.map(_.getPath.toString())
      }

      //val orderPath = FileOrder(filepath)
      val orderPath = filepath

      if (!orderPath.isEmpty) {
        for (i <- 0 to (orderPath.length - 1)) {

          LandingFilePath = orderPath(i)
          sb.append("Processing the file from landed dir : " + LandingFilePath + "\n")

          val FileName = LandingFilePath.split("/").last
         
          if(FileName.toUpperCase().contains("_RESTATEMENT_"))
          {
            mode = "overwrite"
          }
          
          sb.append("Landed File name is : " + FileName + "\n")

          sb.append("Creating a Hashmap to pass as a parameter .......\n")

          val ParamHash: HashMap[String, String] = HashMap[String, String]("NotNullColumn" -> NotNullColumn,
            "SchemaValidationColumn" -> SchemaValidationColumn,
            "UDLpath" -> UDLpath,
            "ErrorPath" -> Res_ErrorPath,
            "header" -> header,
            "mode" -> mode,
            "SplitFileOnDate" -> SplitFileOnDate,
            "SplitDateColumn" -> SplitDateColumn,
            "SingleFile" -> SingleFile,
            "SingleFileName" -> (UDLFileName.replaceAll("YYYYMMDD", DateFolder)),
            "Adl_Path" -> Adl_Path,
            "DataSet_Name" -> DataSet_Name,
            "LandingFilePath" -> LandingFilePath,
            "StartTime" -> StartTime.toString(),
            "duplicateRow" -> duplicateRow,
            "schema" -> schema,
            "ReferentialString" -> ReferentialString,
            "RejectReferentialData" -> RejectReferentialData,
            "mappingPrimaryKey" -> mappingPrimaryKey,
            "mappingFilePath" -> mappingFilePath,
            "HarmonizeDateColumn" -> HarmonizeDateColumn,
            "HarmonizedDateFormat" -> HarmonizedDateFormat,
            "rejectDateHarmonize" -> rejectDateHarmonize,
            "rejectDuplicateData" -> rejectDuplicateData,
            "rejectLengthData" -> rejectLengthData,
            "rejectDecimalData" -> rejectDecimalData,
            "rejectDateData" -> rejectDateData,
            "rejectInteger" -> rejectInteger,
            "lengthOffset" -> lengthOffset)

          sb.append("Hashmap is : " + ParamHash + "\n")

          if (FileName.toLowerCase().contains(".csv")) {
            sb.append("File format is csv\n")
            FileFormat = "com.databricks.spark.csv"
            sb.append("Creating dataframe............\n")

            val LandingDF = spark.read.format(FileFormat).option("header", header).option("delimiter", delimiter).load(LandingFilePath).withColumn("PROCESS_TIMESTAMP", lit(StartTime))

            sb.append("Calling ProcessDF method.........\n")
            
            LandingDF.createOrReplaceTempView("Landing_Table")
           
            val sbb = processDF(spark, hadoopConf, hdfs, ParamHash)
            sb.append(sbb.toString())
          } 
          /*else if (FileName.contains(".xlsx")) {
            sb.append("File format is xlsx\n")
            FileFormat = "com.crealytics.spark.excel"
            sb.append("Creating dataframe\n")

            val Landing_xls_DF = spark.read.format(FileFormat).option("location", LandingFilePath).option("useHeader", header).option("treatEmptyValuesAsNulls", "true").option("inferSchema", "false").option("addColorColumns", "false").load(LandingFilePath)

            sb.append("Calling ProcessDF method.........\n")
            val sbb = processDF(Landing_xls_DF, spark, hadoopConf, hdfs, ParamHash)
            sb.append(sbb.toString())
          }
*/
        }
      }
    } catch {
      case t: Throwable => { sb.append("EXCEPTION OCCURED : " + t.getMessage + "\n") }
    }

    sb
  }

  private def processDF(spark: SparkSession, hadoopConf: Configuration, hdfs: FileSystem, ParamHash: HashMap[String, String]): StringBuffer = {

    var sb: StringBuffer = new StringBuffer("Inside processDF method\n")

    try {
      val NotNullColumn = ParamHash.getOrElse("NotNullColumn", "NA")
      val SchemaValidationColumn = ParamHash.getOrElse("SchemaValidationColumn", "NA")
      val UDLpath = ParamHash.getOrElse("UDLpath", "NA")
      val ErrorPath = ParamHash.getOrElse("ErrorPath", "NA")
      val header = ParamHash.getOrElse("header", "NA")
      var mode = ParamHash.getOrElse("mode", "NA")
      val SplitFileOnDate = ParamHash.getOrElse("SplitFileOnDate", "NA")
      val SplitDateColumn = ParamHash.getOrElse("SplitDateColumn", "NA")
      val SingleFile = ParamHash.getOrElse("SingleFile", "NA")
      val SingleFileName = ParamHash.getOrElse("SingleFileName", "NA")
      val Adl_Path = ParamHash.getOrElse("Adl_Path", "NA")
      val DataSet_Name = ParamHash.getOrElse("DataSet_Name", "NA")
      val LandingFilePath = ParamHash.getOrElse("LandingFilePath", "NA")
      val StartTime = ParamHash.getOrElse("StartTime", "NA")
      val duplicateRow = ParamHash.getOrElse("duplicateRow", "NA")
      val schema = ParamHash.getOrElse("schema", "NA")
      val ReferentialString = ParamHash.getOrElse("ReferentialString", "NA")
      val RejectReferentialData = ParamHash.getOrElse("RejectReferentialData", "NA")
      val mappingPrimaryKey = ParamHash.getOrElse("mappingPrimaryKey", "NA")
      val mappingFilePath = ParamHash.getOrElse("mappingFilePath", "NA")
      val HarmonizeDateColumn = ParamHash.getOrElse("HarmonizeDateColumn", "NA")
      val HarmonizedDateFormat = ParamHash.getOrElse("HarmonizedDateFormat", "ddMMMyyyy")
      val rejectDateHarmonize = ParamHash.getOrElse("rejectDateHarmonize", "NA")
      val rejectDuplicateData = ParamHash.getOrElse("rejectDuplicateData", "NA")
      val rejectLengthData = ParamHash.getOrElse("rejectLengthData", "NA")
      val rejectDecimalData = ParamHash.getOrElse("rejectDecimalData", "NA")
      val rejectDateData = ParamHash.getOrElse("rejectDateData", "NA")
      val rejectInteger = ParamHash.getOrElse("rejectInteger", "NA")
      val lengthOffset = ParamHash.getOrElse("lengthOffset", "0")

      var inputRowCount = 0L
      var ErrorRowCount = 0L
      var UdlRowCount = 0L
      var currentTimestampval = StartTime

      var status = ""
      var Reason = ""

      val LandingDF = spark.sql("SELECT * FROM Landing_Table")
      val inpSchema = LandingDF.schema
      //val ErrorSchema = inpSchema.add(StructField("Rejection_Reason", StringType, true))

      val Inp_Columns = LandingDF.columns.map(_.trim().replaceAll(" ", "_"))
      sb.append("Inp_Columns: "+Inp_Columns.mkString(",")+"\n")
      val exp_Columns = schema.split(",").:+("PROCESS_TIMESTAMP").map(_.trim().replaceAll(" ", "_"))
      sb.append("exp_Columns: "+exp_Columns.mkString(",")+"\n")

      var validateSchema_Flag = false
      
      sb.append("Checking the header.........\n")
      val Inp_Columns_Count = Inp_Columns.length
      val exp_Columns_Count = exp_Columns.length
      
      sb.append("Inp_Columns_Count: " +Inp_Columns_Count+"\n")
      sb.append("exp_Columns_Count: " +exp_Columns_Count+"\n")
      
      validateSchema_Flag = (Inp_Columns_Count == exp_Columns_Count)
      
      if (validateSchema_Flag) {
        sb.append("Header validation passed\n")
        val schema = StructType(exp_Columns.map(a => StructField(a, StringType, true)))
        val InpDF = spark.createDataFrame(LandingDF.rdd, schema)
        sb.append("Creating temp view 'InpTable'.........\n")
        InpDF.createOrReplaceTempView("InpTable")
        InpDF.cache()

        inputRowCount = InpDF.count()

        sb.append("Input file row count " + inputRowCount + "\n")

        val ErrorDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema).withColumn("Rejection_Reason", lit(""))
        sb.append("Creating temp view 'ErrorDF_Table'.........\n")
        ErrorDF.createOrReplaceTempView("ErrorDF_Table")

        val validateArray = validateSchemaString(SchemaValidationColumn)

        
       /* if (!duplicateRow.equals("NA")) {
          sb.append("Checking if duplicate rows present.........\n")
          duplicateRowCheck(duplicateRow, spark, rejectDuplicateData)
        }*/
        //spark.sql("SELECT * FROM InpTable").checkpoint()
        
        if (!validateArray._1.isEmpty) {
          sb.append("Checking if length of columns is correct.........\n")
          lengthCheckSchema(exp_Columns, validateArray._1, spark, rejectLengthData,lengthOffset.toInt)
        }
       //spark.sql("SELECT * FROM InpTable").checkpoint()
        if (!validateArray._2.isEmpty) {
          sb.append("Checking if decimals of columns is correct.........\n")
          DecimalCheck(exp_Columns, validateArray._2, spark, rejectDecimalData)
        }
       //spark.sql("SELECT * FROM InpTable").checkpoint()
        if (!validateArray._3.isEmpty) {
          sb.append("Checking if date format is correct.........\n")
          Datecheck(exp_Columns, validateArray._3, spark, rejectDateData)
        }
        //spark.sql("SELECT * FROM InpTable").checkpoint()
        if (!validateArray._4.isEmpty) {
         sb.append("Checking if Integer format is correct.........\n")
         intCheckSchema(exp_Columns, validateArray._4, spark, rejectInteger)
        }
        //spark.sql("SELECT * FROM InpTable").checkpoint()
        if(!HarmonizeDateColumn.equals("NA"))
        {
          sb.append("Checking for Harmonized date column.........\n")
         DateHarmonize(exp_Columns, HarmonizeDateColumn.split(","), HarmonizedDateFormat, spark, rejectDateHarmonize)
        }
        /*if (!ReferentialString.equals("NA")) {
          sb.append("Checking for referential integrity.........\n")
          referentialValidation(spark, Adl_Path, ReferentialString, RejectReferentialData)
        }*/
        //spark.sql("SELECT * FROM InpTable").checkpoint()
        
        var ColumnSplit = Array[String]()
        
        if(!SplitDateColumn.equals("NA"))
        {
          sb.append("Split date column.........\n")
          ColumnSplit = SplitDateOnColumn(SplitDateColumn.split('|'), spark)
        }
        if(!mappingPrimaryKey.equals("NA"))
        {
          if(!hdfs.isDirectory(new org.apache.hadoop.fs.Path(mappingFilePath)))
          {
            hdfs.mkdirs(new org.apache.hadoop.fs.Path(mappingFilePath))
          }
          sb.append("Executing mapping logic.........\n")
          mappingDataProcess(spark, mappingFilePath+"/"+DataSet_Name+"_Mapping.csv", mappingPrimaryKey.split(","))
          
          val MappingDF = spark.sql("SELECT * FROM MappingTable")
         
          writeDataFrame(MappingDF, mappingFilePath, "true", "append", "false", "true", DataSet_Name+"_Mapping.csv", "true", Adl_Path, hadoopConf, hdfs)
          //sb.append(sbb)
        }
        
        if(mode.equals("overwrite"))
        {
          sb.append("Inside mode change and folder delete..........\n")
          delModeFiles(UDLpath, ColumnSplit, spark, hdfs)
          mode = "append"
        }
        sb.append("Starting op_temp_DF........\n")
        val op_DF = spark.sql("SELECT * FROM InpTable")
        sb.append("Done op_temp_DF........\n")

        op_DF.cache()
        sb.append("Done op_DF cache........\n")
        
        try {
          op_DF.first()
        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }

        sb.append("Starting op_DF.count()........\n")
        UdlRowCount = op_DF.count()
        sb.append("UdlRowCount row count " + UdlRowCount + "\n")

        sb.append("Writing the DF to UDL folder : " + UDLpath + "\n")
        //writeDataFrame(op_DF, UDLpath, header, mode, SplitFileOnDate, SingleFile, SingleFileName,"false", Adl_Path, hadoopConf, hdfs)
        if(ColumnSplit.isEmpty)
        {
          writeTransDataFrame(op_DF.repartition(1), UDLpath, SingleFileName, Array[String](), mode, Adl_Path, hadoopConf, hdfs)
        }
        else
        {
          writeTransDataFrame(op_DF.repartition(1), UDLpath, SingleFileName, ColumnSplit, mode, Adl_Path, hadoopConf, hdfs)
        }
        
        
        val Res_ErrorDF = spark.sql("SELECT * FROM ErrorDF_Table")
        //val Res_ErrorDF = spark.createDataFrame(Res_ErrorDF_temp.rdd, ErrorSchema)

        Res_ErrorDF.cache()
        ErrorRowCount = Res_ErrorDF.count()
        sb.append("ErrorRowCount row count " + ErrorRowCount + "\n")

        //currentTimestampval = currentTimestamp()
        val ErroRecordFileName = "ErrorRecord_" + currentTimestampval + ".csv"
        sb.append("Writing the Error record DF to Error folder : " + ErrorPath + "\n")
        writeDataFrame(Res_ErrorDF, ErrorPath, "true", "append", "false", "true", ErroRecordFileName,"false", Adl_Path, hadoopConf, hdfs)

        status = "PROCESSED"
        Reason = "NA"

      } else {
        status = "REJECTED"
        Reason = "HEADER MISMATCH"
      }
     

      val EndTime = currentTimestamp()
      val ErrorMasterHashParam = HashMap[String, String]("DataSet" -> DataSet_Name,
        "InputFile" -> LandingFilePath,
        "Status" -> status,
        "Reason" -> Reason,
        "InputRowCount" -> inputRowCount.toString(),
        "RejectedRowCount" -> ErrorRowCount.toString,
        "OutputRowCount" -> UdlRowCount.toString,
        "StartTime" -> StartTime,
        "EndTime" -> EndTime)

      val ErrorMasterDF = errorMaster(ErrorMasterHashParam, spark)
      val ErroMasterFileName = "ErrorMaster_" + currentTimestampval + ".csv"
      sb.append("Writing the Error master DF to Error folder : " + ErrorPath + "\n")
      writeDataFrame(ErrorMasterDF, ErrorPath, "true", "append", "false", "true", ErroMasterFileName,"false", Adl_Path, hadoopConf, hdfs)
    } catch {
      case t: Throwable => { sb.append("EXCEPTION OCCURED : " + t.getMessage + " -----------" + t.printStackTrace() + "\n") }
    }
    sb
  }
}