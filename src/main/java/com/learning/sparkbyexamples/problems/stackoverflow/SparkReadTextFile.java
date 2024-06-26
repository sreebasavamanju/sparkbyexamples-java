package com.learning.sparkbyexamples.problems.stackoverflow;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * https://stackoverflow.com/questions/78586247/reading-text-file-in-pyspark-with-delimiters-present-within-double-quotes
 */
public class SparkReadTextFile {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Spark read Text File").master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		
		// we need to use quote option otherwise result wont look like below
		Dataset<Row> df = spark.read().option("header", true).option("sep", "þ").option("quote", "").csv(args[0]);
		df.show(false);
	}
}
