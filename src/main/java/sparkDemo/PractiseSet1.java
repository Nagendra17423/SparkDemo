package sparkDemo;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.collection.parallel.ParIterableLike.Foreach;

public class PractiseSet1 {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("PractiseSet-1").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		boolean testMode = false;

		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);
//		viewData.collect().forEach(str->System.out.print(str+" "));
//		System.out.println();
//		chapterData.collect().forEach(str->System.out.print(str+" "));
//		System.out.println();
//		titlesData.collect().forEach(str->System.out.print(str+" "));
		
//		chapterData.mapToPair(tup->new Tuple2<Integer,Integer>(tup._2,1))
//		.reduceByKey((val1,val2)->val1+val2).collect().forEach(tup->System.out.println(" cousre :"+tup._1+" "+tup._2));
		
		viewData=viewData.distinct();
		System.out.println("*******");
		JavaPairRDD<Integer, Integer> div=chapterData.mapToPair(tup->new Tuple2<Integer,Integer>(tup._2,1)).reduceByKey((val1,val2)->val1+val2);
		
		JavaPairRDD<Tuple2<Integer,Integer>,Long> step3=viewData
				.mapToPair(tup->new Tuple2<Integer,Integer>(tup._2,tup._1)).join(chapterData)
				.mapToPair(tup->new Tuple2<Tuple2<Integer,Integer>,Long>(new Tuple2<Integer,Integer>(tup._2._1,tup._2._2),1l));
//		.mapToPair(tup->new Tuple2<Integer,Tuple2<Integer,Integer>>(tup._2._1,new Tuple2(tup._1,1)))
//		.reduceByKey((tup1,tup2)->new Tuple2<Integer,Integer>(tup1._1,tup1._2+tup2._2))
//		.mapToPair(tup->new Tuple2<Integer,Tuple2<Integer,Integer>>(tup._2._1,new Tuple2(tup._1,tup._2._2)))
//		.collect().forEach(tup->System.out.println(tup._1+" "+tup._2._1+" "+tup._2._2));
				JavaPairRDD<Tuple2<Integer,Integer>,Long> step4=step3.reduceByKey((val1,val2)->val1+val2);
//				.collect().forEach(tup->System.out.println(tup));
//				getting course and respective  no of chapters watched..
				JavaPairRDD<Integer, Long> step5=step4.mapToPair(tup->new Tuple2<Integer,Long>(tup._1._2,tup._2));
				
				JavaPairRDD<Integer,Tuple2<Long,Integer>> step6=step5.join(div);
				JavaPairRDD<Integer, Double> step7=step6.mapValues(value->(double)value._1/value._2);
				JavaPairRDD<Integer,Long> step8=step7.mapValues(value->{
					if(value>0.9)
						return 10l;
					else if(value>0.5)
						return 4l;
					else if(value>0.25)
						return 2l;
					return 0l;
				});
				
				JavaPairRDD<Integer, Long> step9=step8.reduceByKey((val1,val2)->val1+val2);
				JavaPairRDD<Integer,Tuple2<Long,String>> step10=step9.join(titlesData);
				JavaPairRDD<String, Long> step11=step10.mapToPair(tup->new Tuple2<String,Long>(tup._2._2,tup._2._1));
				step11.foreach(str->System.out.println(str));
				
		
		
		
		Scanner scanner=new Scanner(System.in);
		scanner.nextLine();
		sc.close();

	}

	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if(testMode)
		{
			List<Tuple2<Integer, String>> rawTitles = new ArrayList();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
			
		}
		
		return sc.textFile("src/main/resources/titles.csv")
				.mapToPair(record->new Tuple2<Integer,String>(Integer.valueOf(record.split(",")[0]),record.split(",")[1]));
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96,  1));
			rawChapterData.add(new Tuple2<>(97,  1));
			rawChapterData.add(new Tuple2<>(98,  1));
			rawChapterData.add(new Tuple2<>(99,  2));
			rawChapterData.add(new Tuple2<>(100, 3));
			rawChapterData.add(new Tuple2<>(101, 3));
			rawChapterData.add(new Tuple2<>(102, 3));
			rawChapterData.add(new Tuple2<>(103, 3));
			rawChapterData.add(new Tuple2<>(104, 3));
			rawChapterData.add(new Tuple2<>(105, 3));
			rawChapterData.add(new Tuple2<>(106, 3));
			rawChapterData.add(new Tuple2<>(107, 3));
			rawChapterData.add(new Tuple2<>(108, 3));
			rawChapterData.add(new Tuple2<>(109, 3));
			return sc.parallelizePairs(rawChapterData);
		}
		
		return sc.textFile("src/main/resources/chapters.csv")
				.mapToPair(rows->new Tuple2<Integer,Integer>(Integer.valueOf(rows.split(",")[0]),Integer.valueOf(rows.split(",")[1])));
		
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {
		if (testMode)
		{
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(14, 99));
			rawViewData.add(new Tuple2<>(13, 100));
			return  sc.parallelizePairs(rawViewData);
		}
		
		return sc.textFile("src/main/resources/views-*.csv")
			     .mapToPair(commaSeparatedLine -> {
			    	 String[] columns = commaSeparatedLine.split(",");
			    	 return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
			     });
	}

}
