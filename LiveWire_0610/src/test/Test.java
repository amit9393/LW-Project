package test;

import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import org.apache.spark.sql.functions;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import landingtoudl.ResourceFunctions;
import scala.Array;
import scala.collection.immutable.List;

public class Test {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Testing").master("local[*]").getOrCreate();
		
		
		SQLContext sqlContext = new SQLContext(spark);
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		//Dataset<Row> DF1 = new List("").toDF("a");
		ResourceFunctions rf = new ResourceFunctions();
		
		ArrayList<String[]> InputAsList = new ArrayList();
		InputAsList.add("1,Emp,20180512,15000000".split(","));
		InputAsList.add("002,Emp1,20180511,15000000".split(","));
		InputAsList.add("3,Emp2,20170512,15000000".split(","));
		InputAsList.add("ab,Emp4,20180516,15000000".split(","));
		InputAsList.add(",Emp5,20180516,15000000".split(","));
		
		StructType schema = DataTypes.createStructType(new StructField[] { DataTypes.createStructField("id", DataTypes.StringType, true),DataTypes.createStructField("description", DataTypes.StringType, true),DataTypes.createStructField("DateField", DataTypes.StringType, true),DataTypes.createStructField("PROCESS_TIMESTAMP", DataTypes.StringType, true) });
		
		StructType Errorschema = DataTypes.createStructType(new StructField[] { DataTypes.createStructField("id", DataTypes.StringType, true),DataTypes.createStructField("description", DataTypes.StringType, true),DataTypes.createStructField("DateField", DataTypes.StringType, true),DataTypes.createStructField("PROCESS_TIMESTAMP", DataTypes.StringType, true),DataTypes.createStructField("Rejection_Reason", DataTypes.StringType, true) });

		ArrayList<String[]> ErrorList = new ArrayList();
		ErrorList.add("001,Emp,20180512,15000000,Nothing".split(","));
		
		JavaRDD<Row> ErrorRDD = sc.parallelize(ErrorList).map(a -> RowFactory.create(a));   

	    Dataset<Row> ErrorDF = spark.createDataFrame(ErrorRDD, Errorschema);
	    
		ErrorDF.createOrReplaceTempView("ErrorDF_Table");

		JavaRDD<Row> InpRDD = sc.parallelize(InputAsList).map(a -> RowFactory.create(a));   

	    Dataset<Row> InpDF = spark.createDataFrame(InpRDD, schema);   
	    InpDF.createOrReplaceTempView("InpTable");
	    
	    ArrayList<String[]> OutputAsListWoRejection = new ArrayList();
	    OutputAsListWoRejection.add("1,Emp,20180512,15000000".split(","));
	    OutputAsListWoRejection.add("002,Emp1,20180511,15000000".split(","));
	    OutputAsListWoRejection.add("3,Emp2,20170512,15000000".split(","));
	    OutputAsListWoRejection.add("ab,Emp4,20180516,15000000".split(","));
	    OutputAsListWoRejection.add(",Emp5,20180516,15000000".split(","));
		
		JavaRDD<Row> OpWoRDD = sc.parallelize(OutputAsListWoRejection).map(a -> RowFactory.create(a));   

	    Dataset<Row> OpWoDF = spark.createDataFrame(OpWoRDD, schema);
	    
	    ArrayList<String[]> ErrorListWo = new ArrayList();
		ErrorListWo.add("001,Emp,20180512,15000000,Nothing".split(","));
		ErrorListWo.add("ab#Emp4#20180516#15000000#Integer fields not in proper format for column: id-INT".split("#"));
		
	    JavaRDD<Row> ErrorWoRDD = sc.parallelize(ErrorListWo).map(a -> RowFactory.create(a));   

	    Dataset<Row> ErrorWoDF = spark.createDataFrame(ErrorWoRDD, Errorschema);
	    
	    ArrayList<String[]> OutputAsListRejection = new ArrayList();
	    OutputAsListRejection.add("1,Emp,20180512,15000000".split(","));
	    OutputAsListRejection.add("002,Emp1,20180511,15000000".split(","));
	    OutputAsListRejection.add("3,Emp2,20170512,15000000".split(","));
	    OutputAsListRejection.add(",Emp5,20180516,15000000".split(","));
		
		JavaRDD<Row> OpWiRDD = sc.parallelize(OutputAsListRejection).map(a -> RowFactory.create(a));   

	    Dataset<Row> OpWiDF = spark.createDataFrame(OpWiRDD, schema);
	    
	    ArrayList<String[]> ErrorListWi = new ArrayList();
	    ErrorListWi.add("001,Emp,20180512,15000000,Nothing".split(","));
	    ErrorListWi.add("ab#Emp4#20180516#15000000#Integer fields not in proper format for column: id-INT".split("#"));
	    ErrorListWi.add("ab#Emp4#20180516#15000000#Integer fields not in proper format for column: id-INT".split("#"));
	    
	    JavaRDD<Row> ErrorWiRDD = sc.parallelize(ErrorListWi).map(a -> RowFactory.create(a));   

	    Dataset<Row> ErrorWiDF = spark.createDataFrame(ErrorWiRDD, Errorschema);
	    
	    String[] InpCol = InpDF.columns();
	    String[] ValidationColumn = "id-INT".split(",");
	    
	    
	    
	    rf.intCheckSchema(InpCol, ValidationColumn, spark, "false");
	    Dataset<Row> actualWoDF = spark.sql("SELECT * FROM InpTable");
	    Dataset<Row> actualErrorWoDF = spark.sql("SELECT * FROM ErrorDF_Table");
	    
	   Boolean flag1 = rf.compareDataFrame(OpWoDF, actualWoDF);
	    Boolean ErrorFlag1 = rf.compareDataFrame(ErrorWoDF, actualErrorWoDF);
	    
	    
	    rf.intCheckSchema(InpCol, ValidationColumn, spark, "true");
	    Dataset<Row> actualWiDF = spark.sql("SELECT * FROM InpTable");
	    Dataset<Row> actualErrorWiDF = spark.sql("SELECT * FROM ErrorDF_Table");
	    Boolean flag2 = rf.compareDataFrame(OpWiDF, actualWiDF);
	    Boolean ErrorFlag2 = rf.compareDataFrame(ErrorWiDF, actualErrorWiDF);
	    
	    assertTrue(flag1);
	    assertTrue(ErrorFlag1);
	    assertTrue(flag2);
	    assertTrue(ErrorFlag2);
	}

}

