package landingtoudl

import java.util.Date
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.functions._
import scala.collection.immutable.HashMap
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import scala.util.Try
import org.apache.hadoop.fs.FileStatus

class ResourceFunctions2 {
  def delModeFiles(parentPath:String, Columns:Array[String],spark:SparkSession, hdfs:FileSystem):Unit ={
    
    val selectString = Columns.mkString(",")
    val DateFolders = spark.sql("SELECT "+selectString+" FROM InpTable").distinct().collectAsList().iterator()
    
    var i = 0
    while(DateFolders.hasNext())
    {
      val row = DateFolders.next()
      var Folderpath = parentPath
      var i = 0
      for(Date_Col <- Columns)
      {
        Folderpath = Folderpath +"/"+Date_Col+"="+row.get(i)
        i = i+1
      }
      if(hdfs.isDirectory(new org.apache.hadoop.fs.Path(Folderpath)))
      {
        hdfs.delete(new org.apache.hadoop.fs.Path(Folderpath))
      }
      Folderpath = ""
      i = i+1
    }
    
  }
  def SplitDateOnColumn(SplitDateCol:Array[String], spark:SparkSession,sb:StringBuffer):(Array[String],StringBuffer) = 
  {
    val InpDF = spark.sql("SELECT * FROM InpTable")

      val DateCheck = ((date: String, InpFormat: String, OpFormat: String) => {

        var op = "Unparsable"
        try {
          val formatter: SimpleDateFormat = new SimpleDateFormat(InpFormat)
          val opformatter: SimpleDateFormat = new SimpleDateFormat(OpFormat)
          val dt = formatter.parse(date)
          op = opformatter.format(dt)

        } catch {
          case t: Throwable => { //In case of exception 
            if (t.toString().length() > 0) {
              op = "Unparsable"
            }
          }
        }
        op
      })

    spark.udf.register("DateCheck", DateCheck)
    
    val SelectQuery = splitDateString(SplitDateCol,sb)
    val ResDF = spark.sql("SELECT *,"+SelectQuery._1+" FROM InpTable")
    ResDF.createOrReplaceTempView("InpTable")
    (SelectQuery._2.split(","),SelectQuery._3)
  }
  def splitDateString(SplitDateCol: Array[String],sb:StringBuffer): (String, String,StringBuffer) =
    {
      var selectOp = ""
      var ExtraColumn = ""
      for (col <- SplitDateCol) {
        val splitCol = col.split("#")
        
        sb.append("splitCol length :"+splitCol.length)

        if (splitCol.length == 4) {
          val ExtCol = splitCol(0)
          val InpCol = splitCol(1).trim().replaceAll(" ", "_")
          val inpFormat = splitCol(2)
          val opFormat = splitCol(3)
          val sel = "DateCheck(`" + InpCol + "`,'" + inpFormat + "','" + opFormat + "') AS `" + ExtCol + "`,"
          selectOp = selectOp + sel
          ExtraColumn = ExtraColumn + ExtCol + ","
        } else if (splitCol.length == 2) {
          val ExtCol = splitCol(0)
          val InpCol = splitCol(1).trim().replaceAll(" ", "_")

          val sel = "`" + InpCol + "` AS `" + ExtCol + "`,"
          selectOp = selectOp + sel
          ExtraColumn = ExtraColumn + ExtCol + ","
        } else if (splitCol.length == 3) {
          sb.append(" len ==3")
          sb.append("splitCol : "+splitCol.mkString(","))
          val ExtCol = splitCol(0)
          val InpCol = splitCol(1).trim().replaceAll(" ", "_")
          val Function = splitCol(2).trim()
          
          val sel = Function+" AS `" + ExtCol + "`,"
          selectOp = selectOp + sel
          ExtraColumn = ExtraColumn + ExtCol + ","
        }

      }
      
      sb.append("selectOp Start: "+selectOp)
      selectOp = selectOp.substring(0, selectOp.length() - 1)
      sb.append("selectOp done:" + selectOp)
       sb.append("ExtraColumn Start: "+ExtraColumn)
      ExtraColumn = ExtraColumn.substring(0, ExtraColumn.length() - 1)
      sb.append("ExtraColumn done: "+ExtraColumn)
      (selectOp, ExtraColumn,sb)
    }
  def CurrentDate():String = 
  {
    var op = ""
      try {
      op = java.time.LocalDate.now.toString.replaceAll("-","")  
         
      } catch {
        case t: Throwable => { op = "" }
      }
      op
  }  

  def validateSchema(InputColumns: Array[String], ExpectedColumns: Array[String]): Boolean =
    {
      var flag = true

      try {
        flag = InputColumns.sameElements(ExpectedColumns)
      } catch {
        case t: Throwable => { flag = false }
      }
      flag
    }

  /*
	 * The below method: mappingDataProcess will take the delta/incremental data from CSV file
	 *  and append or Update it to the Mapping file
	 * If the mapping file does not exist, it catches the error in catch block
	 
		 */
  def mappingDataProcess(spark: SparkSession, mappingFilePath: String, primaryKey: Array[String]): Unit =
    {
      try {
        //Converts the Primary key column array into String and replaces blanks with underscore in column names
        //Grave accent(`) handles any special character in column names
        val PrimaryKeyColumn = primaryKey.map(a => "`" + a.trim().replaceAll(" ", "_") + "`").mkString(",")

        //Defining Dataframe for Incremental/delta data
        
        val SelectDF = spark.sql("SELECT " + PrimaryKeyColumn + ",PROCESS_TIMESTAMP FROM InpTable")

        val Schema = StructType(primaryKey.map(a => a.trim().replaceAll(" ", "_")).map(a => StructField(a, StringType, true))).add(StructField("PROCESS_TIMESTAMP", StringType, true))
        //Creates an empty mapping file, since the Mapping file doesnt exist at first run
        var MappingDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Schema)
        try {
          //Reads data from the mapping file
          val MappingDFTemp = spark.read.format("com.databricks.spark.csv").option("header", "true").load(mappingFilePath)
          MappingDF = MappingDF.unionAll(MappingDFTemp)
        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }
        val unionDF = MappingDF.unionAll(SelectDF)
        unionDF.createOrReplaceTempView("UnionTable")
        val newMappingDF = spark.sql("SELECT " + PrimaryKeyColumn + ", MAX(PROCESS_TIMESTAMP) AS PROCESS_TIMESTAMP FROM UnionTable GROUP BY " + PrimaryKeyColumn)

        newMappingDF.createOrReplaceTempView("MappingTable")
      } catch {
        case t: Throwable => t.printStackTrace() // TODO: handle error
      }

    }

 
  def currentTimestamp(): String =
    {
      var op = ""
      try {
        val dt: Date = new Date()
        op = dt.getTime.toString()
      } catch {
        case t: Throwable => { op = "" }
      }
      op
    }
  
  def writeTransDataFrame(DF:Dataset[Row],TargetDir:String,FileName:String,SplitColumn:Array[String],mode:String,Adl_Path:String,hadoopConf:Configuration,hdfs:FileSystem):Unit =
  {
    val arrLength = SplitColumn.length
    var listStatus = Array[FileStatus]()
    arrLength match {
        case 0 => {
          DF.write.format("com.databricks.spark.csv").option("header", "true").mode(mode).save(TargetDir)
          listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(Adl_Path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(TargetDir + "/part*.csv".replaceAll(Adl_Path, "")))
        }
        case 1 => {
          DF.write.format("com.databricks.spark.csv").option("header", "true").mode(mode).partitionBy(SplitColumn(0)).save(TargetDir)
          listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(Adl_Path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(TargetDir + "/*/part*.csv".replaceAll(Adl_Path, "")))
        }
        case 2 => {
          DF.write.format("com.databricks.spark.csv").option("header", "true").mode(mode).partitionBy(SplitColumn(0), SplitColumn(1)).save(TargetDir)
        listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(Adl_Path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(TargetDir+"/*/*/part*.csv".replaceAll(Adl_Path, "")))
       }
      case 3 => {
        DF.write.format("com.databricks.spark.csv").option("header", "true").mode(mode).partitionBy(SplitColumn(0),SplitColumn(1),SplitColumn(2)).save(TargetDir)
        listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(Adl_Path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(TargetDir+"/*/*/*/part*.csv".replaceAll(Adl_Path, "")))
      }
      case 4 => {
        DF.write.format("com.databricks.spark.csv").option("header", "true").mode(mode).partitionBy(SplitColumn(0),SplitColumn(1),SplitColumn(2),SplitColumn(3)).save(TargetDir)
        listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(Adl_Path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(TargetDir+"/*/*/*/*/part*.csv".replaceAll(Adl_Path, "")))
      }
       case 5 => {
        DF.write.format("com.databricks.spark.csv").option("header", "true").mode(mode).partitionBy(SplitColumn(0),SplitColumn(1),SplitColumn(2),SplitColumn(3),SplitColumn(4)).save(TargetDir)
        listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(Adl_Path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(TargetDir+"/*/*/*/*/*/part*.csv".replaceAll(Adl_Path, "")))
       }
       case 6 => {
        DF.write.format("com.databricks.spark.csv").option("header", "true").mode(mode).partitionBy(SplitColumn(0),SplitColumn(1),SplitColumn(2),SplitColumn(3),SplitColumn(4),SplitColumn(5)).save(TargetDir)
        listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(Adl_Path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(TargetDir+"/*/*/*/*/*/*/part*.csv".replaceAll(Adl_Path, "")))
       }
       case 7 => {
        DF.write.format("com.databricks.spark.csv").option("header", "true").mode(mode).partitionBy(SplitColumn(0),SplitColumn(1),SplitColumn(2),SplitColumn(3),SplitColumn(4),SplitColumn(5),SplitColumn(6)).save(TargetDir)
        listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(Adl_Path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(TargetDir+"/*/*/*/*/*/*/*/part*.csv".replaceAll(Adl_Path, "")))
       }
       case 8 => {
        DF.write.format("com.databricks.spark.csv").option("header", "true").mode(mode).partitionBy(SplitColumn(0),SplitColumn(1),SplitColumn(2),SplitColumn(3),SplitColumn(4),SplitColumn(5),SplitColumn(6),SplitColumn(7)).save(TargetDir)
        listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(Adl_Path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(TargetDir+"/*/*/*/*/*/*/*/*/part*.csv".replaceAll(Adl_Path, "")))
       }
       case 9 => {
        DF.write.format("com.databricks.spark.csv").option("header", "true").mode(mode).partitionBy(SplitColumn(0),SplitColumn(1),SplitColumn(2),SplitColumn(3),SplitColumn(4),SplitColumn(5),SplitColumn(6),SplitColumn(7),SplitColumn(8)).save(TargetDir)
        listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(Adl_Path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(TargetDir+"/*/*/*/*/*/*/*/*/*/part*.csv".replaceAll(Adl_Path, "")))
       }
       case 10 => {
        DF.write.format("com.databricks.spark.csv").option("header", "true").mode(mode).partitionBy(SplitColumn(0),SplitColumn(1),SplitColumn(2),SplitColumn(3),SplitColumn(4),SplitColumn(5),SplitColumn(6),SplitColumn(7),SplitColumn(8),SplitColumn(9)).save(TargetDir)
        listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(Adl_Path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(TargetDir+"/*/*/*/*/*/*/*/*/*/*/part*.csv".replaceAll(Adl_Path, "")))
       }
       case others => {
         
       }
    }
    
    val arrPath = listStatus.map(_.getPath)
    for(file <- arrPath)
    {
      
    val dt: Date = new Date()
    val curr_timestamp = dt.getTime.toString()
    val withoutLast = file.toString().split('/').dropRight(1).mkString("/")
    hdfs.rename(file, new org.apache.hadoop.fs.Path(withoutLast+"/"+FileName+"_"+curr_timestamp+".csv"))
    }
    
  }

  def writeDataFrame(DF: Dataset[Row], TargetDir: String, header: String, mode: String, SplitFileOnDate: String, SingleFile: String, SingleFileName: String,deleteReplaceFile:String, Adl_Path: String, hadoopConf: Configuration, hdfs: FileSystem): Boolean =
    {
      var flag = false
      try {
        if (SingleFile.equals("false")) {
          if (SplitFileOnDate.equals("false")) {
            DF.write.format("com.databricks.spark.csv").option("header", header.toString()).mode(mode).save(TargetDir)
          } else {
            DF.write.format("com.databricks.spark.csv").option("header", header.toString()).mode(mode).partitionBy("NUM_YEARS", "NUM_MONTH", "NUM_DATE").save(TargetDir)
          }
        } else {
          if (SplitFileOnDate.equals("false")) {
            DF.repartition(1).write.format("com.databricks.spark.csv").option("header", header.toString()).mode(mode).save(TargetDir)
   
            if(deleteReplaceFile.equals("true"))
            {
         
              val listStatusDel = org.apache.hadoop.fs.FileSystem.get(new URI(Adl_Path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path((TargetDir + "/"+SingleFileName.replaceAll(".csv", "*.csv")).replaceAll(Adl_Path, "")))
              if(listStatusDel.length>0)
              {
                val DelFilePath = listStatusDel(0).getPath()
              
              hdfs.delete(DelFilePath)
              }
            }
            val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(Adl_Path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(TargetDir + "/part*.csv".replaceAll(Adl_Path, "")))
            val rename_path = listStatus(0).getPath()
            hdfs.rename(rename_path, new org.apache.hadoop.fs.Path(TargetDir + "/" + SingleFileName))
          } else {
            DF.repartition(1).write.format("com.databricks.spark.csv").option("header", header.toString()).mode(mode).partitionBy("NUM_YEARS", "NUM_MONTH", "NUM_DATE").save(TargetDir)
          }
        }

      } catch {
        case t: Throwable => { flag = false }
      }
      //flag
     flag
    }

  def DeleteMultipleFile(adl_path: String, FilePath: String, hadoopConf: Configuration, hdfs: FileSystem): Boolean =
    {
      var flag = false
      try {
        val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(FilePath.replaceAll(adl_path, "") + "/*"))
        if (!listStatus.isEmpty) {
          for (i <- 0 to (listStatus.length - 1)) {
            val Raw_File_Ind = listStatus.apply(i).getPath.toString()
            flag = hdfs.delete(new org.apache.hadoop.fs.Path(Raw_File_Ind))
          }
        } else {
          flag = true
        }
      } catch {
        case t: Throwable => t.printStackTrace() // TODO: handle error
      }

      return flag
    }


  def errorMaster(ErrorMaster_Map: HashMap[String, String], spark: SparkSession): Dataset[Row] =
    {
      val flag = false

      val DataSet = ErrorMaster_Map.getOrElse("DataSet", "NA")
      val InputFile = ErrorMaster_Map.getOrElse("InputFile", "NA")
      val Status = ErrorMaster_Map.getOrElse("Status", "NA")
      val Reason = ErrorMaster_Map.getOrElse("Reason", "NA")
      val InputRowCount = ErrorMaster_Map.getOrElse("InputRowCount", "NA")
      val RejectedRowCount = ErrorMaster_Map.getOrElse("RejectedRowCount", "NA")
      val OutputRowCount = ErrorMaster_Map.getOrElse("OutputRowCount", "NA")
      val StartTime = ErrorMaster_Map.getOrElse("StartTime", "NA")
      val EndTime = ErrorMaster_Map.getOrElse("EndTime", "NA")

      val Schema = StructType(Array(StructField("DataSet", StringType, true)))

      val newRow = spark.sparkContext.parallelize(Seq(DataSet)).map(t => Row(t))
      val DF = spark.createDataFrame(newRow, Schema)

      val ResDF = DF.withColumn("InputFile", lit(InputFile))
        .withColumn("Status", lit(Status))
        .withColumn("Reason", lit(Reason))
        .withColumn("InputRowCount", lit(InputRowCount))
        .withColumn("RejectedRowCount", lit(RejectedRowCount))
        .withColumn("OutputRowCount", lit(OutputRowCount))
        .withColumn("StartTime", lit(StartTime))
        .withColumn("EndTime", lit(EndTime))
      ResDF
    }

  def referentialValidation(spark: SparkSession,Adl_Path:String, masterString: String, RejectReferentialData : String): Unit =
    {
      val mas = masterString.split(",")
      for (master <- mas) {
        val masterFile = Adl_Path+master.split('|')(0)
        val Columns = master.split('|')(1)

        val columnPair = Columns.split("&")
        var joinCondition = ""
        for (colpar <- columnPair) {
          val transColumn = colpar.split("-")(0).trim().replaceAll(" ", "_")
          val masterColumn = colpar.split("-")(1).trim().replaceAll(" ", "_")
          joinCondition = joinCondition + "InpTable.`" + transColumn + "` <=> MasterDF_table.`" + masterColumn + "` AND "
        }
        joinCondition = joinCondition.substring(0, (joinCondition.length() - 5))

        val MasterDF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ",").load(masterFile)
        MasterDF.createOrReplaceTempView("MasterDF_table")

        val InpDF = spark.sql("SELECT * FROM InpTable")
        val Inp_Col = InpDF.columns.map(a => "InpTable.`" + a + "`").mkString(",")

        val JoinedDF = spark.sql("SELECT " + Inp_Col + " FROM InpTable INNER JOIN MasterDF_table ON " + joinCondition)

        val orgErrorDF = spark.sql("SELECT * FROM ErrorDF_Table")

        val Error_DF_Temp = InpDF.except(JoinedDF)

        val newError_DF = Error_DF_Temp.withColumn("Rejection_Reason", lit("Referential Integrity failed while comparing with master " + masterFile + " on at least one of the following conditions " + columnPair.mkString(",")))

        val Error_DF = orgErrorDF.unionAll(newError_DF)

        Error_DF.createOrReplaceTempView("ErrorDF_Table")
        if(RejectReferentialData.toLowerCase().equals("true"))
        {
          JoinedDF.createOrReplaceTempView("InpTable")
        }
      }
    }

  def duplicateRowCheck(SchemaString_temp: String, spark: SparkSession, rejectDuplicateFlag: String): Unit = {

    val SchemaString_arr = SchemaString_temp.split(",").map(a => a.trim().replaceAll(" ", "_"))
    
    val SchemaString = SchemaString_arr.mkString(",")
    
    val ResSchemaString = SchemaString_arr.map(a => "`" +a+"`").mkString(",")
    
    val inpDF = spark.sql("SELECT * FROM InpTable")
    
    val SelectedDF = spark.sql("SELECT "+ResSchemaString+" FROM InpTable")

    SelectedDF.createOrReplaceTempView("SelectedDF_table")

    val GroupedDf = spark.sql("SELECT " + ResSchemaString + ",COUNT(*) AS COUNT_STAR FROM SelectedDF_table GROUP BY " + ResSchemaString)

    val DuplicateDF = GroupedDf.where(col("COUNT_STAR") > 1).drop("COUNT_STAR")
    
    DuplicateDF.createOrReplaceTempView("DuplicateDF_table")
    
    val DistinctDF = GroupedDf.where(col("COUNT_STAR") === 1).drop("COUNT_STAR")
    
    DistinctDF.createOrReplaceTempView("DistinctDF_table")
    

    val joinString = joinCreate(SchemaString)
    
    val InpCol = inpDF.columns.map(a => "InpTable.`" + a + "`").mkString(",")

    val DuplicateErrorDF = spark.sql("SELECT " + InpCol + " FROM InpTable INNER JOIN DuplicateDF_table ON " + joinString._1)

    //val CleanDF = inpDF.except(DuplicateErrorDF)
    val CleanDF = spark.sql("SELECT " + InpCol + " FROM InpTable INNER JOIN DistinctDF_table ON " + joinString._2)

    val newErrorDF = DuplicateErrorDF.withColumn("Rejection_Reason", lit("Duplicate record found for combination : " + SchemaString))

    val orgErrorDF = spark.sql("SELECT * FROM ErrorDF_Table")
    
    val Error_DF = orgErrorDF.unionAll(newErrorDF)

    Error_DF.createOrReplaceTempView("ErrorDF_Table")

    if (rejectDuplicateFlag.toLowerCase().equals("true")) {
      CleanDF.createOrReplaceTempView("InpTable")
    }
  }
  def joinCreate(SchemaString: String): (String,String) =
    {
      var op = ""
      var op1 = ""
      val SchemaString_array = SchemaString.split(",")
      for (element <- SchemaString_array) {
        op = op+"DuplicateDF_table.`" + element + "` <=> InpTable.`" + element + "` AND "
        op1 = op1+"DistinctDF_table.`" + element + "` <=> InpTable.`" + element + "` AND "
      }
      op = op.substring(0, (op.length() - 5))
      op1 = op1.substring(0, (op1.length() - 5))
      (op,op1)
    }
  def lengthCheckSchema(Inp_Columns: Array[String], SchemaValidationColumn: Array[String], spark: SparkSession, rejectLengthData: String,lengthOffset: Integer): Unit = {

    val lengthCheck = ((ColumnValue: String, Length: Int) => {
      var op = true
      try {
        if (ColumnValue.length() < (Length + 1)) {
          op = true
        } else {
          op = false
        }
      } catch {
        case t: Throwable => { op = true }
      }
      op
    })
  
    spark.udf.register("lengthCheck", lengthCheck)

    val ValidateLengthString = validateLengthSchemaColumn(Inp_Columns, SchemaValidationColumn,lengthOffset)
    
    

    val InpDF = spark.sql("SELECT * FROM InpTable")

    val FilteredDF = spark.sql("SELECT * FROM InpTable WHERE " + ValidateLengthString)

    val orgErrorDF = spark.sql("SELECT * FROM ErrorDF_Table")

    //val newError_DF = InpDF.except(FilteredDF).withColumn("Rejection_Reason", lit("Some Columns of :'" + SchemaValidationColumn.mkString(",") + "' exceeds max length"))
    val newError_DF = spark.sql("SELECT * FROM InpTable WHERE NOT(" + ValidateLengthString+")").withColumn("Rejection_Reason", lit("Some Columns of :'" + SchemaValidationColumn.mkString(",") + "' exceeds max length"))

    val Error_DF = orgErrorDF.unionAll(newError_DF)

    Error_DF.createOrReplaceTempView("ErrorDF_Table")

    if (rejectLengthData.toLowerCase().equals("true")) {
      FilteredDF.createOrReplaceTempView("InpTable")
    }
  }

  def validateLengthSchemaColumn(DataFrame_Columns: Array[String], SchemaValCol: Array[String],lengthOffset: Integer): String = {

    val SchemaValCol_arr_temp = SchemaValCol.map(a => a.replaceAll(" ", "_"))
    val SchemaValCol_arr = SchemaValCol_arr_temp.map(a => a.trim())

    val Col_SchemaVal_Names = SchemaValCol_arr.map(a => a.split("-")(0).trim())
    val Col_SchemaVal_Types = SchemaValCol_arr.map(a => a.split("-")(1).trim())

    var whereOp = ""
    
   var i=0
    for (col <- Col_SchemaVal_Names) {
      val colName = col.trim()

      if (DataFrame_Columns.contains(colName)) {
        
        val index = Col_SchemaVal_Names.indexOf(colName)
        val colType = Col_SchemaVal_Types(index)

        val length = Integer.parseInt(colType.replaceAll("VARCHAR\\(", "").replaceAll("\\)", "")) + lengthOffset
        whereOp = whereOp + "lengthCheck(`" + colName + "`," + length + ") AND "
//        sb.append("whereOp: " +whereOp+"\n")
      }
//      i = i+1
//       sb.append("ValidateLengthString done: " +i+"\n")
      

    }
    whereOp = whereOp.substring(0, (whereOp.length() - 5))
    whereOp
  }
  
  def DateHarmonize(InpColumn: Array[String], DateColumn: Array[String],outFormat:String,spark:SparkSession,rejectDateHarmonize:String):Unit={
    val OutputFormat = new SimpleDateFormat(outFormat)
    
    val HarmonizeDate = ((DateField:String, InFormat : String) => {
      var op = DateField
      try {
        val InputFormat = new SimpleDateFormat(InFormat)
        InputFormat.setLenient(false);
        if(InFormat.length() == DateField.length())
        {
          val dt = InputFormat.parse(DateField)
          op = OutputFormat.format(dt)
        }
      } catch {
        case t: Throwable => {op = DateField}
      }
      op
    })
    
    val HarmonizeDateCheck = ((DateField:String, InFormat : String) => {
      var op = false
      try {
        val InputFormat = new SimpleDateFormat(InFormat)
        InputFormat.setLenient(false);
        if(InFormat.length() == DateField.length())
        {
          InputFormat.parse(DateField)
          op = true
        }
      } catch {
        case t: Throwable => {op = false}
      }
      op
    })
    
    spark.udf.register("HarmonizeDateCheck",HarmonizeDateCheck)
    spark.udf.register("HarmonizeDate",HarmonizeDate)
    
    val Query = DateHarmonizeQueryCreate(InpColumn: Array[String], DateColumn: Array[String],outFormat)
    
    val InpDF = spark.sql("SELECT * FROM InpTable")
    
    val SelectedDF = spark.sql("SELECT "+Query._1+" FROM InpTable")
    SelectedDF.createOrReplaceTempView("SelectedDF_table")
    
    val WhereDF = spark.sql("SELECT * FROM SelectedDF_table WHERE "+Query._3)
    
    val oldErrorDF = spark.sql("SELECT * FROM ErrorDF_Table")
    
    //val newErrorDF = SelectedDF.except(WhereDF).withColumn("Rejection_Reason", lit("Date harmonization failed for column : "+DateColumn.mkString(",")))
    val newErrorDF = spark.sql("SELECT * FROM InpTable WHERE NOT("+Query._2+")").withColumn("Rejection_Reason", lit("Date harmonization failed for column : "+DateColumn.mkString(",")))
     
    val Error_DF = oldErrorDF.unionAll(newErrorDF)

    Error_DF.createOrReplaceTempView("ErrorDF_Table")
    
    if (rejectDateHarmonize.toLowerCase().equals("true")) {
      WhereDF.createOrReplaceTempView("InpTable")
    }
    else
    {
      SelectedDF.createOrReplaceTempView("InpTable")
    }
    
  }
  
  def DateHarmonizeQueryCreate(InpColumn: Array[String], DateColumn: Array[String],op_format:String):(String,String,String)=
  {
    var selectop = ""
    var whereop = ""
    var whereop2 = ""
    try {
      val SchemaValCol_arr_temp = DateColumn.map(a => a.replaceAll(" ", "_"))
      val SchemaValCol_arr = SchemaValCol_arr_temp.map(a => a.trim())
      
      val Datearr = SchemaValCol_arr.map(a => (a.split("#")(0),a.split("#")(1)))
      val DateColName = Datearr.map(a => a._1)

      for(incol <- InpColumn)
      {
        if(DateColName.contains(incol))
        {
          val DateVar = Datearr(DateColName.indexOf(incol))
          selectop = selectop + "HarmonizeDate(`"+DateVar._1+"`,'"+DateVar._2+"') AS `"+DateVar._1+"`,"
          whereop = whereop + "HarmonizeDateCheck(`"+DateVar._1+"`,'"+DateVar._2+"') AND "
          whereop2 = whereop2 + "HarmonizeDateCheck(`"+DateVar._1+"`,'"+op_format+"') AND "
        }
        else
        {
          selectop = selectop +"`"+incol+"`,"
        }
      }
      selectop = selectop.substring(0, (selectop.length() - 1))
       whereop = whereop.substring(0, (whereop.length() - 5))
       whereop2 = whereop2.substring(0, (whereop2.length() - 5))
      
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
    (selectop,whereop,whereop2)
  }

  def Datecheck(InpColumn: Array[String], DateColumn: Array[String], spark: SparkSession, rejectDateData: String): Unit = {
    val validateDate = ((strDate: String, format: String, nullable:String) => {
      var flag = false
   
      if ((strDate == null || strDate.isEmpty() || strDate.equals("") || strDate.equals("00000000") || strDate.equals("0")) && nullable.toLowerCase().equals("true"))
      {
        flag = true
      }
      else {
        val sdfrmt = new SimpleDateFormat(format);
        sdfrmt.setLenient(false);
        try {
          if(strDate.length() == format.length())
          {
            val ProcessedDate = sdfrmt.parse(strDate.trim());
            flag = true
          }
         
        } catch {
          case t: Throwable => { flag = false }
        }
      }
    flag
    })

    spark.udf.register("validateDate", validateDate)

    val whereop = DateFormatCheck(InpColumn, DateColumn)

    val InpDF = spark.sql("SELECT * FROM InpTable")

    val FilteredDF = spark.sql("SELECT * FROM InpTable WHERE " + whereop)

    val orgErrorDF = spark.sql("SELECT * FROM ErrorDF_Table")

    //val newError_DF = InpDF.except(FilteredDF).withColumn("Rejection_Reason", lit("Date fields not in proper format"))
    val newError_DF = spark.sql("SELECT * FROM InpTable WHERE NOT(" + whereop+")").withColumn("Rejection_Reason", lit("Date fields: "+DateColumn.mkString(",")+" not in proper format"))

    val Error_DF = orgErrorDF.unionAll(newError_DF)

    Error_DF.createOrReplaceTempView("ErrorDF_Table")

    if (rejectDateData.toLowerCase().equals("true")) {
      FilteredDF.createOrReplaceTempView("InpTable")
    }

  }

  def DateFormatCheck(InpColumn: Array[String], SchemaValCol: Array[String]): String = {

    val SchemaValCol_arr_temp = SchemaValCol.map(a => a.replaceAll(" ", "_"))
    val SchemaValCol_arr = SchemaValCol_arr_temp.map(a => a.trim())

    val datefields = SchemaValCol_arr.map(a => (a.split("#")(0).trim(), a.split("#")(1).trim().replaceAll("DATE\\(", "").replaceAll("\\)", "")))

    var whereOp = ""

    for (datefield <- datefields) {
      if (InpColumn.contains(datefield._1)) {
        val format = datefield._2.split(",")(0)
        val nullable = datefield._2.split(",")(1)
        whereOp = whereOp + "validateDate(`" + datefield._1 + "`,'" + format + "','"+nullable+"') AND "
      }
    }
    whereOp = whereOp.substring(0, (whereOp.length() - 5))
    whereOp
  }

  def intCheckSchema(DataFrame_Columns: Array[String], SchemaValCol: Array[String], spark: SparkSession, rejectInteger:String): Unit = {

    val checkInt = ((Value: String) => {
      var flag = true
    var Value_Final = Value
    try {
      if(!(Value_Final == null || Value_Final.isEmpty() || Value_Final.equals("")))
    {
        if(Value_Final.reverse.charAt(0) == '-')
        {
          Value_Final = "-"+Value_Final.substring(0, Value_Final.length()-1)
        }
     flag = Try(Value_Final.toInt).isSuccess
    }
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
    flag
    })
    
    val IntMinus = ((Value:String) => {
      var Value_Final = Value
    try {
      if(!(Value_Final == null || Value_Final.isEmpty() || Value_Final.equals("")))
    {
        if(Value_Final.reverse.charAt(0) == '-')
        {
          Value_Final = "-"+Value_Final.substring(0, Value_Final.length()-1)
        }
    }
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
    Value_Final
    })

    spark.udf.register("checkInt", checkInt)
    spark.udf.register("IntMinus", IntMinus)
    
    val InpDF = spark.sql("SELECT * FROM InpTable")

    val Query = validateIntegerSchemaColumn(DataFrame_Columns, SchemaValCol)
    
    val cleanDF = spark.sql("SELECT * FROM InpTable WHERE " + Query._2)
    cleanDF.createOrReplaceTempView("CleanInt_Table")
    
    val orgErrorDF = spark.sql("SELECT * FROM ErrorDF_Table")

    //val newError_DF = InpDF.except(cleanDF).withColumn("Rejection_Reason", lit("Integer fields not in proper format for column: "+SchemaValCol))
    val newError_DF = spark.sql("SELECT * FROM InpTable WHERE NOT(" + Query._2+")").withColumn("Rejection_Reason", lit("Integer fields not in proper format for column: "+SchemaValCol.mkString(",")))

    val Error_DF = orgErrorDF.unionAll(newError_DF)

    Error_DF.createOrReplaceTempView("ErrorDF_Table")
    
     if (rejectInteger.toLowerCase().equals("true")) {
      val ResDF = spark.sql("SELECT "+Query._1+" FROM CleanInt_Table")
      ResDF.createOrReplaceTempView("InpTable")
    }
     else
     {
       val ResDF = spark.sql("SELECT "+Query._1+" FROM InpTable")
       ResDF.createOrReplaceTempView("InpTable")
     }
  }

  def validateIntegerSchemaColumn(DataFrame_Columns: Array[String], SchemaValCol: Array[String]): (String,String) = {

    val SchemaValCol_arr_temp = SchemaValCol.map(a => a.replaceAll(" ", "_"))
    val SchemaValCol_arr = SchemaValCol_arr_temp.map(a => a.trim())

    val Col_SchemaVal_Names = SchemaValCol_arr.map(a => a.split("-")(0).trim())

    var SelectOp = ""
    var whereOp = ""

    for (col <- DataFrame_Columns) {
      val colName = col.trim()
      if (Col_SchemaVal_Names.contains(colName)) {
        SelectOp = SelectOp + "IntMinus(`"+colName+"`) AS `"+colName+"`,"
        whereOp = whereOp + "checkInt(`" + colName + "`) AND "
      }
      else
      {
        SelectOp = SelectOp + "`"+colName+"` AS `"+colName+"`,"
      }
    }
    SelectOp = SelectOp.substring(0, (SelectOp.length() - 1))
    whereOp = whereOp.substring(0, (whereOp.length() - 5))
    (SelectOp,whereOp)
  }

  def validateSchemaString(SchemaString: String): (Array[String], Array[String], Array[String], Array[String]) =
    {
      val SchemaArray = SchemaString.split('|')
      var LengthCheck = Array[String]()
      var DecimalCheck = Array[String]()
      var DateCheck = Array[String]()
      var IntCheck = Array[String]()

      for (element <- SchemaArray) {
        if (element.toUpperCase().contains("-VARCHAR")) {
          LengthCheck = LengthCheck.:+(element)
        } else if (element.toUpperCase().contains("-DECIMAL")) {
          DecimalCheck = DecimalCheck.:+(element)
        } else if (element.toUpperCase().contains("#DATE")) {
          DateCheck = DateCheck.:+(element)
        } else if (element.toUpperCase().contains("-INT")) {
          IntCheck = IntCheck.:+(element)
        }
      }
      (LengthCheck, DecimalCheck, DateCheck, IntCheck)
    }

  def DecimalCheck(InpColumn: Array[String], decimalColumn: Array[String], spark: SparkSession, rejectDecimalData: String): Unit = {

    try {
      val decround = ((decimalCol: String, secondpart: Int) => {
        var res = ""

        try {
          var InpData = decimalCol.trim()
          if(InpData.reverse.charAt(0) == '-')
          {
            InpData = "-"+InpData.substring(0, InpData.length()-1)
          }
          res = BigDecimal(InpData).setScale(secondpart, BigDecimal.RoundingMode.HALF_UP).toDouble.toString()

          val secondPart_length = res.split('.')(1).length()

          if (secondPart_length < secondpart) {
            var zeroString = ""
            for (i <- 1 to secondpart - secondPart_length) { zeroString = zeroString + "0" }

            res = res + zeroString
          }
        } catch {
          case t: Throwable => res = decimalCol // TODO: handle error
        }
        res
      })

      val deccheck = ((decimalCol: String, firstpart: Int, secondpart: Int) => {
        var flag = false
        try {
          var firstpart_Final = firstpart
          if(decimalCol == null || decimalCol.isEmpty() || decimalCol.equals(""))
          {
            flag = true
          }
          else
          {
            var decimalCol_trim = decimalCol.trim()
            if(decimalCol_trim.reverse.charAt(0) == '-')
          {
            decimalCol_trim = "-"+decimalCol_trim.substring(0, decimalCol_trim.length()-1)
            
          }
            if(decimalCol_trim.charAt(0) == '-')
            {
              firstpart_Final = firstpart_Final+1
            }
            val precsion = decimalCol_trim.split('.')
          val first = precsion(0)
          decimalCol_trim.toDouble
          if (first.length > (firstpart_Final - secondpart)) {
            flag = false
          } else {
            flag = true
          }

          }
          
        } catch {
          case t: Throwable => flag = false // TODO: handle error
        }
        flag
      })

      spark.udf.register("decround", decround)

      spark.udf.register("deccheck", deccheck)
      
      val ValidateDecimal = DecimalFormatCheck(InpColumn, decimalColumn)
      
      val InputDf = spark.sql("SELECT * from InpTable")
      
      val ResDF = spark.sql("SELECT " + ValidateDecimal._1 + " FROM InpTable")
      
      val WhereDF = spark.sql("SELECT * FROM InpTable WHERE " + ValidateDecimal._2)
      WhereDF.createOrReplaceTempView("WhereDF_table")
      
      val orgErrorDF = spark.sql("SELECT * FROM ErrorDF_Table")

      //val Error_DF_Temp = InputDf.except(WhereDF)
      val Error_DF_Temp = spark.sql("SELECT * FROM InpTable WHERE NOT(" + ValidateDecimal._2+")")
      
      val newError_DF = Error_DF_Temp.withColumn("Rejection_Reason", lit("Decimal columns " + decimalColumn.mkString(",") + " not in proper format"))
      val Error_DF = orgErrorDF.unionAll(newError_DF)
      
      Error_DF.createOrReplaceTempView("ErrorDF_Table")
      
      if (rejectDecimalData.toLowerCase().equals("true")) {
        spark.sql("SELECT " + ValidateDecimal._1 + " FROM WhereDF_table").createOrReplaceTempView("InpTable")
      }
      else
      {
        ResDF.createOrReplaceTempView("InpTable")
      }
    } catch {
      case t: Throwable => {}
    }
  }

  def DecimalFormatCheck(InpColumn: Array[String], SchemaValCol: Array[String]): (String, String) = {

    var selectOp = ""
    var whereOp = ""
    try {
      val SchemaValCol_arr_temp = SchemaValCol.map(a => a.replaceAll(" ", "_")) //Nielsen_Category-DECIMAL(4,2)
      val SchemaValCol_arr = SchemaValCol_arr_temp.map(a => a.trim())

      val decimalfields_names = SchemaValCol_arr.map(a => (a.split("-")(0).trim()))
      val decimalfields_values = SchemaValCol_arr.map(a => (a.split("-")(1).trim().replaceAll("DECIMAL\\(", "").replaceAll("\\)", "").split(",")))

      for (inp <- InpColumn) {

        if (decimalfields_names.contains(inp)) {

          val index = decimalfields_names.indexOf(inp)
          val values = decimalfields_values(index)
          selectOp = selectOp + "decround(`" + inp + "`," + values(1) + ") AS `" + inp + "`,"
          whereOp = whereOp + "deccheck(`" + inp + "`," + values(0) + "," + values(1) + ") AND "
        } else {
          selectOp = selectOp +"`"+ inp + "`,"
        }
      }

      selectOp = selectOp.substring(0, (selectOp.length() - 1))
      whereOp = whereOp.substring(0, (whereOp.length() - 5))

    } catch {
      case t: Throwable => {t.printStackTrace()}
    }
    (selectOp, whereOp)
  }

  def FileOrder(filepath: Array[String]): Array[String] = {
    val datedarray = filepath.map(a => (a, a.split("/").last.split("_").last.substring(0, 8).toInt))
    val sortarray = datedarray.sortBy(a => a._2).map(a => a._1)
    sortarray
  }

  def getConfiguration(ConfigPath: String, dataset: String, spark: SparkSession): java.util.HashMap[String, String] = {

    val opParam = new java.util.HashMap[String, String]()

    try {
      val js_rdd = spark.sparkContext.wholeTextFiles(ConfigPath).values

      val DF = spark.read.json(js_rdd)

      val Res_DF = DF.select(dataset)
        .withColumn("Exp", explode(col(dataset)))
        .withColumn("Key", col("Exp")(0))
        .withColumn("Value", col("Exp")(1))
        .drop(dataset).drop("Exp")

      val ResArray = Res_DF.collect()

      for (row <- ResArray) {
        val Key = row(0).toString()
        val Value = row(1).toString()

        opParam.put(Key, Value)
      }
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }

    opParam
  }
  
 def getConfigurationArray(ConfigPath: String, dataset: String, spark: SparkSession): Array[(String,String)] = {

    var opParam = Array[(String,String)]()

    try {
      val js_rdd = spark.sparkContext.wholeTextFiles(ConfigPath).values

      val DF = spark.read.json(js_rdd)

      val Res_DF = DF.select(dataset)
        .withColumn("Exp", explode(col(dataset)))
        .withColumn("Key", col("Exp")(0))
        .withColumn("Value", col("Exp")(1))
        .drop(dataset).drop("Exp")

      val ResArray = Res_DF.collect()

      for (row <- ResArray) {
        val Key = row(0).toString()
        val Value = row(1).toString()

        opParam = opParam.:+(Key, Value)
      }
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }

    opParam
  }
 def timeSpanCalculate(AggCol:Array[String], CombinationColumn:Array[String],DateColumn_Tup:(String,String),TimeSpan_StartDT_Tup:(String,String),TimeSpan_EndDT_Tup:(String,String),ResTableName : String, spark:SparkSession):Unit = 
  {
  
    val Date_to_Timestamp = ((DateField:String, format:String) => {
      var op = 0L
      try {
        val inpFormat:SimpleDateFormat = new SimpleDateFormat(format)
        val dt = inpFormat.parse(DateField)
        op = dt.getTime
      } catch {
        case t: Throwable => op = 0L
      }
      op
    })
    
    spark.udf.register("Date_to_Timestamp", Date_to_Timestamp)

    val DateColumn = DateColumn_Tup._1
      
    val InpDF = spark.sql("SELECT *,Date_to_Timestamp(`"+DateColumn_Tup._1+"`,'"+DateColumn_Tup._2+"') AS `NUM_ProcessDate` FROM FactTable")
    
    InpDF.createOrReplaceTempView("DateConvInp_Table")

    val sqlQueryStatements = timeSpan_query(AggCol, CombinationColumn, TimeSpan_StartDT_Tup,TimeSpan_EndDT_Tup)
  
    //val TimespanStart_End = spark.sql("SELECT Date_to_Timestamp(`L3M_Start`,'yyyyMMdd') AS `NUM_StartDT`,Date_to_Timestamp(`L3M_End`,'yyyyMMdd') AS `NUM_EndDT`, L3M_Start FROM Date_Table").distinct()
    val TimespanStart_End = spark.sql("SELECT "+sqlQueryStatements._1+" FROM Date_Table").distinct()
    TimespanStart_End.createOrReplaceTempView("TimespanStart_End")
    
   val Inp_join_startend = spark.sql("SELECT "+sqlQueryStatements._2+" FROM DateConvInp_Table AS F1 LEFT JOIN TimespanStart_End AS F2 ON F1.NUM_ProcessDate BETWEEN F2.`NUM_StartDT` AND F2.`NUM_EndDT`")
    Inp_join_startend.createOrReplaceTempView("Inp_join_startend_Table")
   
    val ResDF = spark.sql("SELECT "+sqlQueryStatements._4+" FROM Inp_join_startend_Table GROUP BY "+sqlQueryStatements._5)

    ResDF.createOrReplaceTempView(ResTableName)
  }
 
 
  def timeSpan_query(AggCol_Type:Array[String], CombinationColumn:Array[String],TimeSpanStart:(String,String),TimeSpanEnd:(String,String)):(String,String,String,String,String) = 
  {
    
    val AggCol_Final = AggCol_Type.map(col => ("`"+col.split('-')(0).trim().replaceAll(" ", "_")+"`",col.split('-')(1).trim()))
    
      val CombinationColumn_Final = CombinationColumn.map(col => "`"+col.trim().replaceAll(" ", "_")+"`")
      
      val SelectOp_1 = "Date_to_Timestamp(`"+TimeSpanStart._1+"`,'"+TimeSpanStart._2+"') AS `NUM_StartDT`,Date_to_Timestamp(`"+TimeSpanEnd._1+"`,'"+TimeSpanEnd._2+"') AS `NUM_EndDT`,`"+TimeSpanStart._1+"`"
      
      val SelectOp_2 = AggCol_Final.map(col => "F1."+col._1).mkString(",")+","+CombinationColumn_Final.map(col => "F1."+col).mkString(",")+","+("F2.`"+TimeSpanStart._1+"`")
    
      val JoinCondition = CombinationColumn_Final.map(col => "F1."+col+" = F2."+col).mkString(" AND ")
      
      val SelectOp_3 = CombinationColumn_Final.mkString(",")+",DateConvert(`"+TimeSpanStart._1+"`,'"+TimeSpanStart._2+"') AS `"+TimeSpanStart._1+"`,"+AggCol_Final.map(col => "SUM(CAST("+col._1+" AS "+col._2+")) AS "+col._1).mkString(",")
      //val SelectOp_3 = CombinationColumn_Final.mkString(",")+",`"+TimeSpanStart._1+"`"+","+AggCol_Final.map(col => "SUM(CAST("+col._1+" AS "+col._2+")) AS "+col._1).mkString(",")
      //val WhereCondition = "F2."+DateColumn_Final+ " BETWEEN F1.`"+TimeSpan_StartDT+"` AND F1.`"+TimeSpan_EndDT+"`"
      
     
      val GroupBy = CombinationColumn_Final.mkString(",")+",`"+TimeSpanStart._1+"`"
      
      (SelectOp_1,SelectOp_2,JoinCondition,SelectOp_3,GroupBy)
  }
  
  def timeSpan_process(AggCol_Type:Array[String], CombinationColumn:Array[String], DateColumn_Tup:(String,String),spark:SparkSession):Unit = 
  {
    //val AggCol = AggCol_Type.map(a => a.split('-')(0).trim.replaceAll(" ", "_"))
    
    
    val opDateFormat = new SimpleDateFormat("yyyyMMdd")
    
    val DateConvert = ((DateInp:String,inpFormat:String) => {
      var op = ""
      try {
        val inpDateFormat = new SimpleDateFormat(inpFormat)
        op = opDateFormat.format(inpDateFormat.parse(DateInp))
      } catch {
        case t: Throwable => t.printStackTrace() // TODO: handle error
      }
      op
    })
    
    spark.udf.register("DateConvert",DateConvert)
    
    val L3M = (("L3M_START","ddMMMyyyy"),("MAT_L3M_END","ddMMMyyyy"),"L3M_table")
    val MAT = (("MAT_START","ddMMMyyyy"),("MAT_L3M_END","ddMMMyyyy"),"MAT_table")
    val MTD = (("FIRST_DAY_MONTH","ddMMMyyyy"),("TIME_KEY","yyyyMMdd"),"MTD_table")
		val Month = (("FIRST_DAY_LAST_MONTH","ddMMMyyyy"),("MAT_L3M_END","ddMMMyyyy"),"Month_table")
    val QTD = (("FIRST_DAY_QUARTER","ddMMMyyyy"),("TIME_KEY","yyyyMMdd"),"QTD_table")
	  val YTD = (("FIRST_DAY_YEAR","ddMMMyyyy"),("TIME_KEY","yyyyMMdd"),"YTD_table")
		val Year = (("FIRST_DAY_LAST_YEAR","ddMMMyyyy"),("YEAR_END","ddMMMyyyy"),"Year_table")
		val Quarter = (("FIRST_DAY_LAST_QUARTER","ddMMMyyyy"),("LAST_DAY_LAST_QUARTER","ddMMMyyyy"),"Quarter_table")
			 
    
    val Arr_TimespanType:Array[((String,String),(String,String),String)] = Array(L3M,MAT,MTD,Month,YTD,QTD,Year,Quarter)
    
    for(timespan_type <- Arr_TimespanType)
    {
      timeSpanCalculate(AggCol_Type,CombinationColumn,DateColumn_Tup,timespan_type._1,timespan_type._2,timespan_type._3,spark)
    }
    
    val Clean_CombinationCol = CombinationColumn.map(a => "`"+a+"`")
    //val Clean_AggCol = AggCol.map(a => "`"+a+"`")
    val Clean_AggCol = AggCol_Type.map(a => ("`"+a.split('-')(0).trim.replaceAll(" ", "_")+"`", a.split('-')(1).trim.replaceAll(" ", "_")))
    val Clean_DateCol = "`"+DateColumn_Tup._1+"`"
      
    //val factDF = spark.sql("SELECT "+Clean_CombinationCol.mkString(",")+",DateConvert("+Clean_DateCol+") AS `TIME_KEY`, "+Clean_AggCol.mkString(",")+", 'DAYS' AS `TIMEPERIOD_TYPE` FROM FactTable")
    //val factDF = spark.sql("SELECT "+Clean_CombinationCol.mkString(",")+",DateConvert("+Clean_DateCol+") AS `TIME_KEY`, "+Clean_AggCol.map(a => "CAST("+a._1+" AS "+a._2+") AS "+a._1).mkString(",")+", 'DAYS' AS `TIMEPERIOD_TYPE` FROM FactTable")
    val factDF = spark.sql("SELECT "+Clean_CombinationCol.mkString(",")+",DateConvert("+Clean_DateCol+",'"+DateColumn_Tup._2+"') AS `TIME_KEY`, "+Clean_AggCol.map(a => "CAST("+a._1+" AS "+a._2+") AS "+a._1).mkString(",")+", 'DAYS' AS `TIMEPERIOD_TYPE` FROM FactTable")
    
    val L3M_DF = spark.sql("SELECT * FROM L3M_table").withColumnRenamed("L3M_START", "TIME_KEY").withColumn("TIMEPERIOD_TYPE", lit("L3M"))
    val MAT_DF = spark.sql("SELECT * FROM MAT_table").withColumnRenamed("MAT_START", "TIME_KEY").withColumn("TIMEPERIOD_TYPE", lit("MAT"))
    val MTD_DF = spark.sql("SELECT * FROM MTD_table").withColumnRenamed("FIRST_DAY_MONTH", "TIME_KEY").withColumn("TIMEPERIOD_TYPE", lit("MTD"))
    val Month_DF = spark.sql("SELECT * FROM Month_table").withColumnRenamed("FIRST_DAY_LAST_MONTH", "TIME_KEY").withColumn("TIMEPERIOD_TYPE", lit("MONTH"))
    val QTD_DF = spark.sql("SELECT * FROM QTD_table").withColumnRenamed("FIRST_DAY_QUARTER", "TIME_KEY").withColumn("TIMEPERIOD_TYPE", lit("QTD"))
    val YTD_DF = spark.sql("SELECT * FROM YTD_table").withColumnRenamed("FIRST_DAY_YEAR", "TIME_KEY").withColumn("TIMEPERIOD_TYPE", lit("YTD"))
    val Year_DF = spark.sql("SELECT * FROM Year_table").withColumnRenamed("FIRST_DAY_LAST_YEAR", "TIME_KEY").withColumn("TIMEPERIOD_TYPE", lit("YEAR"))
    val Quarter_DF = spark.sql("SELECT * FROM Quarter_table").withColumnRenamed("FIRST_DAY_LAST_YEAR", "TIME_KEY").withColumn("TIMEPERIOD_TYPE", lit("QUARTER"))
      
    /*val CurrentDt = CurrentDate()
    val resDF_temp = factDF.unionAll(L3M_DF).unionAll(MAT_DF).unionAll(MTD_DF).unionAll(Month_DF).unionAll(QTD_DF).unionAll(Year_DF).unionAll(Quarter_DF).withColumn("CREATED_DT", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN"))
    resDF_temp.createOrReplaceTempView("resDF_temp_table")
    
    val resDF = spark.sql("SELECT a.*, time.`TIMEPERIOD_TYPE_ID` FROM resDF_temp_table AS a LEFT JOIN dimTimePeriod_Table AS time ON a.TIMEPERIOD_TYPE = upper(time.`TIMEPERIOD_TYPE`)").drop("TIMEPERIOD_TYPE")
    resDF.createOrReplaceTempView("op_table")*/
    
    
    factDF.createOrReplaceTempView("factDF_table")
    spark.sql("SELECT a.*, time.`TIMEPERIOD_TYPE_ID` FROM factDF_table AS a LEFT JOIN dimTimePeriod_Table AS time ON a.TIMEPERIOD_TYPE = upper(time.`TIMEPERIOD_TYPE`)").drop("TIMEPERIOD_TYPE").createOrReplaceTempView("res_factDF_table")

    L3M_DF.createOrReplaceTempView("L3M_DF_table")
    spark.sql("SELECT a.*, time.`TIMEPERIOD_TYPE_ID` FROM L3M_DF_table AS a LEFT JOIN dimTimePeriod_Table AS time ON a.TIMEPERIOD_TYPE = upper(time.`TIMEPERIOD_TYPE`)").drop("TIMEPERIOD_TYPE").createOrReplaceTempView("res_L3M_DF")
    
    MAT_DF.createOrReplaceTempView("MAT_DF_table")
    spark.sql("SELECT a.*, time.`TIMEPERIOD_TYPE_ID` FROM MAT_DF_table AS a LEFT JOIN dimTimePeriod_Table AS time ON a.TIMEPERIOD_TYPE = upper(time.`TIMEPERIOD_TYPE`)").drop("TIMEPERIOD_TYPE").createOrReplaceTempView("res_MAT_DF_table")
    
    MTD_DF.createOrReplaceTempView("MTD_DF_table")
    spark.sql("SELECT a.*, time.`TIMEPERIOD_TYPE_ID` FROM MTD_DF_table AS a LEFT JOIN dimTimePeriod_Table AS time ON a.TIMEPERIOD_TYPE = upper(time.`TIMEPERIOD_TYPE`)").drop("TIMEPERIOD_TYPE").createOrReplaceTempView("res_MTD_DF_table")
    
    Month_DF.createOrReplaceTempView("Month_DF_table")
    spark.sql("SELECT a.*, time.`TIMEPERIOD_TYPE_ID` FROM Month_DF_table AS a LEFT JOIN dimTimePeriod_Table AS time ON a.TIMEPERIOD_TYPE = upper(time.`TIMEPERIOD_TYPE`)").drop("TIMEPERIOD_TYPE").createOrReplaceTempView("res_Month_DF_table")
    
    YTD_DF.createOrReplaceTempView("YTD_DF_table")
    spark.sql("SELECT a.*, time.`TIMEPERIOD_TYPE_ID` FROM YTD_DF_table AS a LEFT JOIN dimTimePeriod_Table AS time ON a.TIMEPERIOD_TYPE = upper(time.`TIMEPERIOD_TYPE`)").drop("TIMEPERIOD_TYPE").createOrReplaceTempView("res_YTD_DF_table")
    
    QTD_DF.createOrReplaceTempView("QTD_DF_table")
    spark.sql("SELECT a.*, time.`TIMEPERIOD_TYPE_ID` FROM QTD_DF_table AS a LEFT JOIN dimTimePeriod_Table AS time ON a.TIMEPERIOD_TYPE = upper(time.`TIMEPERIOD_TYPE`)").drop("TIMEPERIOD_TYPE").createOrReplaceTempView("res_QTD_DF_table")
    
    Year_DF.createOrReplaceTempView("Year_DF_table")
    spark.sql("SELECT a.*, time.`TIMEPERIOD_TYPE_ID` FROM Year_DF_table AS a LEFT JOIN dimTimePeriod_Table AS time ON a.TIMEPERIOD_TYPE = upper(time.`TIMEPERIOD_TYPE`)").drop("TIMEPERIOD_TYPE").createOrReplaceTempView("res_Year_DF_table")
    
    Quarter_DF.createOrReplaceTempView("Quarter_DF_table")
    spark.sql("SELECT a.*, time.`TIMEPERIOD_TYPE_ID` FROM Quarter_DF_table AS a LEFT JOIN dimTimePeriod_Table AS time ON a.TIMEPERIOD_TYPE = upper(time.`TIMEPERIOD_TYPE`)").drop("TIMEPERIOD_TYPE").createOrReplaceTempView("res_Quarter_DF_table")
    
  }
  
  def timeSpan_process_temp(AggCol_Type:Array[String], CombinationColumn_Days:Array[String],CombinationColumn_Others:Array[String], DateColumn_Tup:(String,String),ParameteDate:(String,String),spark:SparkSession):Unit = 
  {
    val opDateFormat = new SimpleDateFormat("yyyyMMdd")
    
    val DateConvert = ((DateInp:String,inpFormat:String) => {
      var op = ""
      try {
        val inpDateFormat = new SimpleDateFormat(inpFormat)
        op = opDateFormat.format(inpDateFormat.parse(DateInp))
      } catch {
        case t: Throwable => t.printStackTrace() // TODO: handle error
      }
      op
    })
    
    spark.udf.register("DateConvert",DateConvert)
    
    val L3M = (("L3M_START","ddMMMyyyy"),("MAT_L3M_END","ddMMMyyyy"),"L3M_table")
    val MAT = (("MAT_START","ddMMMyyyy"),("MAT_L3M_END","ddMMMyyyy"),"MAT_table")
    
    val MTD = (("FIRST_DAY_MONTH","ddMMMyyyy"),"MTD_table")
		val Month = (("FIRST_DAY_MONTH","ddMMMyyyy"),"Month_table")
    val QTD = (("FIRST_DAY_QUARTER","ddMMMyyyy"),"QTD_table")
    val Quarter = (("FIRST_DAY_QUARTER","ddMMMyyyy"),"Quarter_table")
	  val YTD = (("FIRST_DAY_YEAR","ddMMMyyyy"),"YTD_table")
		val Year = (("FIRST_DAY_YEAR","ddMMMyyyy"),"Year_table")
		
			 
    
    val Arr_TimespanType:Array[((String,String),(String,String),String)] = Array(L3M,MAT)
    
    for(timespan_type <- Arr_TimespanType)
    {
      timeSpanCalculate(AggCol_Type,CombinationColumn_Others,DateColumn_Tup,timespan_type._1,timespan_type._2,timespan_type._3,spark)
    }
    
    val Arr_TimespanType_others:Array[((String,String),String)] = Array(MTD,Month,QTD,YTD,Year,Quarter)
    
    for(timespan_type <- Arr_TimespanType_others)
    {
      timeSpanCalculate_temp(AggCol_Type, CombinationColumn_Others, DateColumn_Tup, timespan_type._1, ParameteDate, timespan_type._2, spark)
    }
    
    val Clean_CombinationCol = CombinationColumn_Days.map(a => "`"+a+"`")

    val Clean_AggCol = AggCol_Type.map(a => ("`"+a.split('-')(0).trim.replaceAll(" ", "_")+"`", a.split('-')(1).trim.replaceAll(" ", "_")))
    val Clean_DateCol = "`"+DateColumn_Tup._1+"`"
      
    val factDF = spark.sql("SELECT "+Clean_CombinationCol.mkString(",")+",DateConvert("+Clean_DateCol+",'"+DateColumn_Tup._2+"') AS `TIME_KEY`, "+Clean_AggCol.map(a => "CAST("+a._1+" AS "+a._2+") AS "+a._1).mkString(",")+", 'DAYS' AS `TIMEPERIOD_TYPE` FROM FactTable")
    
    val L3M_DF = spark.sql("SELECT * FROM L3M_table").withColumnRenamed("L3M_START", "TIME_KEY").withColumn("TIMEPERIOD_TYPE", lit("L3M"))
    val MAT_DF = spark.sql("SELECT * FROM MAT_table").withColumnRenamed("MAT_START", "TIME_KEY").withColumn("TIMEPERIOD_TYPE", lit("MAT"))
    val MTD_DF = spark.sql("SELECT * FROM MTD_table").withColumnRenamed("FIRST_DAY_MONTH", "TIME_KEY").withColumn("TIMEPERIOD_TYPE", lit("MTD"))
    val Month_DF = spark.sql("SELECT * FROM Month_table").withColumnRenamed("FIRST_DAY_MONTH", "TIME_KEY").withColumn("TIMEPERIOD_TYPE", lit("MONTH"))
    val QTD_DF = spark.sql("SELECT * FROM QTD_table").withColumnRenamed("FIRST_DAY_QUARTER", "TIME_KEY").withColumn("TIMEPERIOD_TYPE", lit("QTD"))
    val Quarter_DF = spark.sql("SELECT * FROM Quarter_table").withColumnRenamed("FIRST_DAY_QUARTER", "TIME_KEY").withColumn("TIMEPERIOD_TYPE", lit("QUARTER"))
    val YTD_DF = spark.sql("SELECT * FROM YTD_table").withColumnRenamed("FIRST_DAY_YEAR", "TIME_KEY").withColumn("TIMEPERIOD_TYPE", lit("YTD"))
    val Year_DF = spark.sql("SELECT * FROM Year_table").withColumnRenamed("FIRST_DAY_YEAR", "TIME_KEY").withColumn("TIMEPERIOD_TYPE", lit("YEAR"))
    
      
    /*val CurrentDt = CurrentDate()
    val resDF_temp = factDF.unionAll(L3M_DF).unionAll(MAT_DF).unionAll(MTD_DF).unionAll(Month_DF).unionAll(QTD_DF).unionAll(Year_DF).unionAll(Quarter_DF).withColumn("CREATED_DT", lit(CurrentDt)).withColumn("CREATED_BY", lit("SPARK_ADMIN"))
    resDF_temp.createOrReplaceTempView("resDF_temp_table")
    
    val resDF = spark.sql("SELECT a.*, time.`TIMEPERIOD_TYPE_ID` FROM resDF_temp_table AS a LEFT JOIN dimTimePeriod_Table AS time ON a.TIMEPERIOD_TYPE = upper(time.`TIMEPERIOD_TYPE`)").drop("TIMEPERIOD_TYPE")
    resDF.createOrReplaceTempView("op_table")*/
    
    
    factDF.createOrReplaceTempView("factDF_table")
    spark.sql("SELECT a.*, time.`TIMEPERIOD_TYPE_ID` FROM factDF_table AS a LEFT JOIN dimTimePeriod_Table AS time ON a.TIMEPERIOD_TYPE = upper(time.`TIMEPERIOD_TYPE`)").drop("TIMEPERIOD_TYPE").createOrReplaceTempView("res_factDF_table")

    L3M_DF.createOrReplaceTempView("L3M_DF_table")
    spark.sql("SELECT a.*, time.`TIMEPERIOD_TYPE_ID` FROM L3M_DF_table AS a LEFT JOIN dimTimePeriod_Table AS time ON a.TIMEPERIOD_TYPE = upper(time.`TIMEPERIOD_TYPE`)").drop("TIMEPERIOD_TYPE").createOrReplaceTempView("res_L3M_DF")
    
    MAT_DF.createOrReplaceTempView("MAT_DF_table")
    spark.sql("SELECT a.*, time.`TIMEPERIOD_TYPE_ID` FROM MAT_DF_table AS a LEFT JOIN dimTimePeriod_Table AS time ON a.TIMEPERIOD_TYPE = upper(time.`TIMEPERIOD_TYPE`)").drop("TIMEPERIOD_TYPE").createOrReplaceTempView("res_MAT_DF_table")
    
    MTD_DF.createOrReplaceTempView("MTD_DF_table")
    spark.sql("SELECT a.*, time.`TIMEPERIOD_TYPE_ID` FROM MTD_DF_table AS a LEFT JOIN dimTimePeriod_Table AS time ON a.TIMEPERIOD_TYPE = upper(time.`TIMEPERIOD_TYPE`)").drop("TIMEPERIOD_TYPE").createOrReplaceTempView("res_MTD_DF_table")
    
    Month_DF.createOrReplaceTempView("Month_DF_table")
    spark.sql("SELECT a.*, time.`TIMEPERIOD_TYPE_ID` FROM Month_DF_table AS a LEFT JOIN dimTimePeriod_Table AS time ON a.TIMEPERIOD_TYPE = upper(time.`TIMEPERIOD_TYPE`)").drop("TIMEPERIOD_TYPE").createOrReplaceTempView("res_Month_DF_table")
    
    YTD_DF.createOrReplaceTempView("YTD_DF_table")
    spark.sql("SELECT a.*, time.`TIMEPERIOD_TYPE_ID` FROM YTD_DF_table AS a LEFT JOIN dimTimePeriod_Table AS time ON a.TIMEPERIOD_TYPE = upper(time.`TIMEPERIOD_TYPE`)").drop("TIMEPERIOD_TYPE").createOrReplaceTempView("res_YTD_DF_table")
    
    QTD_DF.createOrReplaceTempView("QTD_DF_table")
    spark.sql("SELECT a.*, time.`TIMEPERIOD_TYPE_ID` FROM QTD_DF_table AS a LEFT JOIN dimTimePeriod_Table AS time ON a.TIMEPERIOD_TYPE = upper(time.`TIMEPERIOD_TYPE`)").drop("TIMEPERIOD_TYPE").createOrReplaceTempView("res_QTD_DF_table")
    
    Year_DF.createOrReplaceTempView("Year_DF_table")
    spark.sql("SELECT a.*, time.`TIMEPERIOD_TYPE_ID` FROM Year_DF_table AS a LEFT JOIN dimTimePeriod_Table AS time ON a.TIMEPERIOD_TYPE = upper(time.`TIMEPERIOD_TYPE`)").drop("TIMEPERIOD_TYPE").createOrReplaceTempView("res_Year_DF_table")
    
    Quarter_DF.createOrReplaceTempView("Quarter_DF_table")
    spark.sql("SELECT a.*, time.`TIMEPERIOD_TYPE_ID` FROM Quarter_DF_table AS a LEFT JOIN dimTimePeriod_Table AS time ON a.TIMEPERIOD_TYPE = upper(time.`TIMEPERIOD_TYPE`)").drop("TIMEPERIOD_TYPE").createOrReplaceTempView("res_Quarter_DF_table")
    
  }
  
   def timeSpanCalculate_temp(AggCol:Array[String], CombinationColumn:Array[String],DateColumn_Tup:(String,String),TimeSpan_StartDT_Tup:(String,String),TimeSpan_TimeKey_Tup:(String,String),ResTableName : String, spark:SparkSession):Unit = 
  {
     val Date_to_Timestamp = ((DateField:String, format:String) => {
      var op = 0L
      try {
        val inpFormat:SimpleDateFormat = new SimpleDateFormat(format)
        val dt = inpFormat.parse(DateField)
        op = dt.getTime
      } catch {
        case t: Throwable => op = 0L
      }
      op
    })
    
    spark.udf.register("Date_to_Timestamp", Date_to_Timestamp)
    
    val sqlQueryStatements = timeSpan_query_temp(AggCol, CombinationColumn, TimeSpan_StartDT_Tup)
  
   val Inp_join_startend = spark.sql("SELECT "+sqlQueryStatements._1+" FROM FactTable AS F1 LEFT JOIN Date_Table AS F2 ON Date_to_Timestamp(F1.`"+DateColumn_Tup._1+"`,'"+DateColumn_Tup._2+"') = Date_to_Timestamp(F2.`"+TimeSpan_TimeKey_Tup._1+"`,'"+TimeSpan_TimeKey_Tup._2+"')")
    Inp_join_startend.createOrReplaceTempView("Inp_join_startend_Table")
   println("Inp_join_startend: "+"SELECT "+sqlQueryStatements._1+" FROM FactTable AS F1 LEFT JOIN Date_Table AS F2 ON F1.`"+DateColumn_Tup._1+"` = F2.`"+TimeSpan_TimeKey_Tup._1+"`")
    
   val ResDF = spark.sql("SELECT "+sqlQueryStatements._2+" FROM Inp_join_startend_Table GROUP BY "+sqlQueryStatements._3)
println("ResDF: "+"SELECT "+sqlQueryStatements._2+" FROM Inp_join_startend_Table GROUP BY "+sqlQueryStatements._3)
    ResDF.createOrReplaceTempView(ResTableName)
  }
   
   def timeSpan_query_temp(AggCol_Type:Array[String], CombinationColumn:Array[String],TimeSpanStart:(String,String)):(String,String,String) = 
  {
    
    val AggCol_Final = AggCol_Type.map(col => ("`"+col.split('-')(0).trim().replaceAll(" ", "_")+"`",col.split('-')(1).trim()))
    
      val CombinationColumn_Final = CombinationColumn.map(col => "`"+col.trim().replaceAll(" ", "_")+"`")
      
      val SelectOp_1 = AggCol_Final.map(col => "F1."+col._1).mkString(",")+","+CombinationColumn_Final.map(col => "F1."+col).mkString(",")+","+("F2.`"+TimeSpanStart._1+"`")
    
      val SelectOp_2 = CombinationColumn_Final.mkString(",")+",DateConvert(`"+TimeSpanStart._1+"`,'"+TimeSpanStart._2+"') AS `"+TimeSpanStart._1+"`,"+AggCol_Final.map(col => "SUM(CAST("+col._1+" AS "+col._2+")) AS "+col._1).mkString(",")
      
      val GroupBy = CombinationColumn_Final.mkString(",")+",`"+TimeSpanStart._1+"`"
      
      (SelectOp_1,SelectOp_2,GroupBy)
  }
 
  def processQueries(ConfigHashMap: Array[(String, String)],spark:SparkSession):Unit = 
  {
    for(key <- ConfigHashMap)
    {
      val Type = key._1.split('|')(0)
      
      if(Type.toUpperCase().equals("READ"))
      {
        val tableName = key._1.split('|')(1)
        
        val path = key._2
        val DF = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ",").load(path)
       
        DF.createOrReplaceTempView(tableName)
      }
      else if(Type.toUpperCase().equals("QUERY"))
      {
        val tableName = key._1.split('|')(1)
         
        val query = key._2
        val DF = spark.sql(query)
        DF.createOrReplaceTempView(tableName)
      }
      else if(Type.toUpperCase().equals("WRITE"))
      {
        val tableName = key._1.split('|')(1)
        val destination = key._2
        val DF = spark.sql("SELECT * FROM "+tableName)
        DF.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ",").save(destination)
      }
     
    }
  }
  def compareDataFrame(ExpectedDF : Dataset[Row], ActualDF : Dataset[Row]) : Boolean =
  {
    ExpectedDF.cache()
    ActualDF.cache()
    var flag = false
    try {
      val Difference1 = ExpectedDF.except(ActualDF).count()
      val Difference2 = ActualDF.except(ExpectedDF).count()
      
      val sum = Difference1+Difference2
      if(sum == 0)
      {
        flag = true
      }
    } catch {
      case t: Throwable => {flag = false}
    }
    flag
  }
}