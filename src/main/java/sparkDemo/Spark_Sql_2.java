package sparkDemo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Spark_Sql_2 {

	public static void main(String[] args) {
		
		SparkSession spark=SparkSession.builder().appName("spark2").master("local[*]").getOrCreate();
		
		
		
//		StructType strucType=new StructType(new StructField[] {
//				new StructField("Student_id",DataTypes.IntegerType,false,Metadata.empty()),
//				new StructField("Exam_center_id",DataTypes.IntegerType,false,Metadata.empty()),
//				new StructField("Subject",DataTypes.StringType,false,Metadata.empty()),
//				new StructField("Year",DataTypes.IntegerType,false,Metadata.empty()),
//				new StructField("Quater",DataTypes.IntegerType,false,Metadata.empty()),
//				new StructField("Score",DataTypes.IntegerType,false,Metadata.empty()),
//				new StructField("Grade",DataTypes.StringType,false,Metadata.empty()),
//		});
		
		Dataset<Row> df1=spark.read().option("header",true)
//				.option("inferSchema",true)
				.csv("C:\\Users\\Nagendra\\eclipse-workspace\\sparkExample\\src\\main\\resources\\students.csv");
		
//		Dataset<Row> df1=spark.read().csv("C:\\Users\\Nagendra\\eclipse-workspace\\sparkExample\\src\\main\\resources\\students.csv");
//		df1=spark.createDataFrame(df1.javaRDD(), strucType);
		df1.printSchema();
		
//		df1
//		.select(col("Subject"),col("SCORE")) 
//		.groupBy(col("Subject")).agg(max(col("Score").cast(DataTypes.IntegerType)),min(col("Score").cast(DataTypes.IntegerType))).show();
//		df1.
//		select(col("student_id"),col("subject").cast(DataTypes.StringType),col("Score").cast(DataTypes.IntegerType))
//		.groupBy(col("student_id")).pivot("subject").agg(max(col("score"))).show();
//		.agg(max(col("score")),min(col("score"))).show();
		
//		df1.select(col("year"),col("subject"),col("score").cast(DataTypes.IntegerType))
//		.groupBy(col("subject")).pivot("year")
//		.agg(round(avg(col("score"))),round(stddev(col("score")))).show();
		
		spark.udf().register("callUdf",(String str,String str1)->
		{
			if(str1.equals("Biology"))
			{
				return str.startsWith("A");
			}
		return str.startsWith("A+") || str.endsWith("+");
		},DataTypes.BooleanType);
		
//		df1.withColumn("passFunc",lit(col("grade").equalTo("A+"))).show();
		df1.withColumn("passFunc",callUDF("callUdf",functions.column("grade"),col("subject"))).show();
		
//		df1.show();
		
	}
}