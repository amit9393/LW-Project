package landingtoudl

import org.apache.spark.sql.SparkSession
import java.util.HashMap

object LandingToUDLprocessing {
  def main(args: Array[String]): Unit = {

    var sb: StringBuffer = new StringBuffer("Inside the method: LandingToUDLprocessing.main \n")

    try {
      //val DataSet = "Order"
      val DataSet = args(0)
      sb.append("Value of Dataset parameter is : " + DataSet + " \n")
      
      val Adl_Path = args(1)
      sb.append("Value of Adl_Path parameter is : " + Adl_Path + " \n")
      

      val ConfigFile = args(2)
      
      
      
      sb.append("Value of ConfigFile parameter is : " + ConfigFile + " \n")
      
      val DateFolder = args(3)
      sb.append("Value of DateFolder parameter is : " + DateFolder + " \n")
      
      sb.append("Starting the Spark Session ..............\n")
      val spark = SparkSession.builder().appName("LandingToUDL").config("spark.executor.cores",5).config("spark.executor.instances", 18).getOrCreate()
      //.config("spark.executor.instances", 30)
      import spark.implicits._
      
      sb.append("Spark Session successfully started\n")

      //val jsonRead: Json = new Json()
      val resource: ResourceFunctions = new ResourceFunctions()
      sb.append("Calling the getConfiguration method ........\n")

      val paramHashMap: HashMap[String, String] = resource.getConfiguration(ConfigFile,DataSet, spark)
      sb.append("Received a HashMap from getConfiguration method\n")
      sb.append("paramHashMap : " + paramHashMap.toString() + "\n")

      sb.append("Adding the Adl_Path,DateFolder,DataSet to paramHashMap ...........\n")
      paramHashMap.put("Adl_Path", Adl_Path)
      paramHashMap.put("DateFolder", DateFolder)
      paramHashMap.put("DataSet", DataSet)
     

      sb.append("Creating hadoopConf and hdfs ..........\n")
      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(Adl_Path), hadoopConf)

      sb.append("Starting actual logic for processing ..........\n")
      if (paramHashMap.get("processClass").equals("Mapping")) {
        sb.append("As processClass = Mapping, Hence inside Mapping IF block \n")
        val Mapping_Process: Mapping = new Mapping()
        sb.append("Calling processMapping method.............. \n")
        val sbb = Mapping_Process.processMapping(paramHashMap, spark, hadoopConf, hdfs)
        sb.append(sbb.toString())
      }
      else if (paramHashMap.get("processClass").equals("Transaction")) {
        sb.append("As processClass = Transaction, Hence inside Transaction IF block \n")
        val Transaction_Process: Transaction = new Transaction()
        sb.append("Calling processTransaction method.............. \n")
        val sbb = Transaction_Process.processTransaction(paramHashMap, spark, hadoopConf, hdfs)
        sb.append(sbb.toString())
      }

    } catch {
      case t: Throwable => { sb.append("EXCEPTION OCCURED : " + t.getMessage + "\n")}
    }

    println(sb.toString())
   
  }
}