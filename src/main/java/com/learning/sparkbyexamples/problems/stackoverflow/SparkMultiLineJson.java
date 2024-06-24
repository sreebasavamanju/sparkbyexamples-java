package com.learning.sparkbyexamples.problems.stackoverflow;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * https://stackoverflow.com/questions/38545850/read-multiline-json-in-apache-spark
 */
public class SparkMultiLineJson {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Spark Read josn Multi line").master("local[*]")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		Dataset<Row> jsonDF = spark.read().option("multiLine", true).json(args[0]);
		jsonDF.show();

	}
}
