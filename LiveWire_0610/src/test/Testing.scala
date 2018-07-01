package test

import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import landingtoudl.ResourceFunctions
import java.util.Date
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import java.util.HashMap
import landingtoudl.ResourceFunctions
import org.apache.spark.sql.Dataset
import scala.util.Try

object Testing {
  def main(args: Array[String]): Unit = {
    
//  val InpColumn = "Col1,Col2,/BIC/Col3,Col4".split(",")
//		val DateColumn = "Col1#yyyyMMdd,/BIC/Col3#yyyyMMdd".split(",")
//    println(DateHarmonizeQueryCreate(InpColumn,DateColumn))
    val inp = "1/1/2016"
    val  InFormat = "dd/MM/yyyy"
    val outFormat = "yyyyMMdd"
    
   val InputFormat = new SimpleDateFormat(InFormat)
        val OutputFormat = new SimpleDateFormat(outFormat)
        InputFormat.setLenient(false);
       
          val dt = InputFormat.parse(inp)
         val  op = OutputFormat.format(dt)
          
          println(op)
    
    
    
    
    
  }
  
  def DateHarmonizeQueryCreate(InpColumn: Array[String], DateColumn: Array[String]):(String,String)=
  {
    var selectop = ""
    var whereop = ""
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
          whereop = whereop + "`"+DateVar._1+"` != 'Unparcable' AND "
        }
        else
        {
          selectop = selectop +"`"+incol+"`,"
        }
      }
      selectop = selectop.substring(0, (selectop.length() - 1))
       whereop = whereop.substring(0, (whereop.length() - 5))
      
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
    (selectop,whereop)
  }
}
