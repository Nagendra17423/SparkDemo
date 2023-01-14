package sparkDemo;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.parallel.ParIterableLike.GroupBy;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.AnalysisException;

public class SparkSql_1 {
	
	public static void main(String[] args) throws AnalysisException {
		
		SparkSession spark=SparkSession.builder().appName("App1").master("local[*]").getOrCreate();
		
		Dataset<Row> df = spark.read().option("header",true).csv("src/main/resources/titles.csv");
//		df=df.filter("serial_id >3 and Title like '%Spring%' ");
		
//		df=df.filter(row->Integer.valueOf(row.get(0)+"")>3 && (row.getAs("Title")+"").contains("Spring"));
		
//		df=df.filter(row->Integer.valueOf(row.get(0)+"")>3 && (row.getAs("Title")+"").contains("Spring"));
		
//		Column Serial_id=df.col("Serial_id");
//		Column title=df.col("Title");
//		df=df.filter(Serial_id.gt("3").and(title.like("%Spring%")));
//		functions.col("Serial_id")
//		df=df.filter(col("Serial_id").geq("3").and(col("title").like("%Spring%")));
		
//		df.createOrReplaceTempView("title_table");
//		
//		spark.sql("select title,dense_rank() over(order by serial_id) as rank_check from title_table group by title,serial_id order by title")
//		.show();
		
		List<Row> list=new ArrayList<Row>();
		list.add(RowFactory.create("WARN","2022-07-06 "));
		list.add(RowFactory.create("WARN","2022-09-18 "));
		list.add(RowFactory.create("ERR","2022-08-08"));
		list.add(RowFactory.create("ERR","2022-07-16 "));
		list.add(RowFactory.create("WARN","2022-08-25"));
		list.add(RowFactory.create("FATAL","2022-08-25"));
		list.add(RowFactory.create("DEBUG","2022-08-25"));
		
		Dataset<Row> df2=spark.createDataFrame(list, new StructType(new StructField[] {
				new StructField("Str1",DataTypes.StringType,false,Metadata.empty()),
				new StructField("Str2",DataTypes.StringType,false,Metadata.empty())
		}
				)
				);
		
		
		df2.createOrReplaceTempView("df2_tab");
		
//		spark.sql("select str1,collect_set(str2) from df2_tab group by str1").show();
		spark.sql("select str1,date_format(str2,'M') from df2_tab group by str1,2").show();
//		df2.show();
		
//		df2.selectExpr("str1","date_format(str2,'MM')").show();
//		df2.select(col("str1"),date_format(col("str2"),"MM")).show();
//		df2.describe("str1").show();
		
//		df2.drop("str1").show();
//		Object row=df2.collect();
//		System.out.println(row);
		
		
//		for(Row row:df2.collectAsList())
//		{
//			System.out.println("row level: "+row.getString(0)+" date time "+row.getAs("Str2"));
//		}
		
		
		
		
//		df2.explain();
		
//		df2.select(col("str1"),date_format(col("str2"),"MMMM").alias("month"),
//				date_format(col("str2"),"M").alias("monthNum").cast(DataTypes.IntegerType))
//		.groupBy(col("str1"),col("month"),col("monthNum")).count()
//		.orderBy(col("monthNum"))
//		.drop(col("monthNum")).show();
		List<Object> l=new ArrayList<>(Arrays.asList(new String[] {"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"}));
		
		df2.select(col("str1"),
				date_format(col("str2"),"MMM").alias("month")
				,date_format(col("str2"),"M").cast(DataTypes.IntegerType)
		.alias("monthNum")).groupBy(col("str1")).pivot("month",l).count().na().fill(0).show();

		
		
		
		
		
		
//		df.show();
		spark.close();
		
	}

}
