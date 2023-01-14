package sparkDemo;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class DemoSpark1 {
public static void main(String[] args) {
		
		SparkConf conf=new SparkConf().setAppName("PairedRdd").setMaster("local[1]");
		JavaSparkContext sc=new JavaSparkContext(conf);
		
//		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		List<String> list=new ArrayList<>();
		list.add("ERR: WEDNESDAY 4 2022");
		list.add("WARN: THURSDAY 2 2022");
		list.add("WARN: MONDAY 1 2022");
		list.add("ERR: SATURDAY 5 2022");
		JavaRDD<String> rdd=sc.parallelize(list);
		
		/*JavaPairRDD<String,Long> pairRdd=rdd.mapToPair(line->{
			
			String arr[]=line.split(":");
			String key=arr[0];
			String value=arr[1];
			return new Tuple2<>(key,1l);
			
		});

		
	    JavaPairRDD<String,Long> pairRdd1=pairRdd.reduceByKey((val1,val2)->val1+val2);
		pairRdd1.foreach(tup->{System.out.println(tup._1+" "+tup._2);}); */
		
		
//		List<Tuple2<String,Long>> res=rdd.mapToPair((val)->new Tuple2<String,Long>(val.split(":")[0],1l))
//		.reduceByKey((val1,val2)-> val1 + val2)
//		.collect();
//		
//		rdd.mapToPair(d->{return new Tuple2<String,String>(d.split(":")[0],d.split(":")[1]);})
//			.groupByKey()
//			.collect()
//			.forEach(tup->{System.out.println(tup._1 +" "+tup._2);});
		
		
		rdd.mapToPair(str->new Tuple2<String,Long>(str.split(":")[0],1l))
		.reduceByKey((val1,val2)->{
			return val1+val2;
		})
		.foreach(tup->{System.out.println(tup._1+" "+tup._2);});
		
		sc.close();
		
		
	}
}
