package com.learning.sparkbyexamples.algorithms;

import java.util.Arrays;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class WordCountUsingRDD {

	public static void main(String[] args) {
		String path = args[0];
		SparkSession spark = SparkSession.builder().appName("WordCount").master("local[*]").getOrCreate();
		SparkContext sc = spark.sparkContext();
		JavaSparkContext jsc = new JavaSparkContext(sc);
		JavaRDD<String> textFile = jsc.textFile(path);
		JavaRDD<String> flatMap = textFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
		JavaPairRDD<String, Integer> mapToPair = flatMap.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
		mapToPair.reduceByKey((x, y) -> x + y).mapToPair(pair -> new Tuple2<Integer, String>(pair._2, pair._1))
				.sortByKey(false).foreach(System.out::println);

	}
}
