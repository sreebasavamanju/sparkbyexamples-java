package com.learning.sparkbyexamples.problems;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class SparkConvertSingleLIneIntoRecords {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Spark Travel Data").master("local[*]").getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");

		Dataset<Row> df = spark.read().csv(args[0]);
		df.printSchema();
		df.show();

		//df = df.withColumn("Array", functions.regex_replace);
		df.show();

		df.select(df.col("_c0"), functions.explode(df.col("Array"))).show();

	}
}
