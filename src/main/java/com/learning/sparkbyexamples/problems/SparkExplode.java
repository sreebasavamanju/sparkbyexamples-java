package com.learning.sparkbyexamples.problems;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class SparkExplode {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Spark Json Array to explode").master("local[*]")
				.getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");

		// data/department.json
		Dataset<Row> jsonDF = spark.read().json(args[0]);
		jsonDF.show(false);

		jsonDF.select(jsonDF.col("dept_id"), functions.explode(jsonDF.col("e_id")).as("e_id")).show();
	}
}
