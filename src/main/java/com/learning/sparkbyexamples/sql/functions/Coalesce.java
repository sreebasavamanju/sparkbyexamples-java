package com.learning.sparkbyexamples.sql.functions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * https://spark.apache.org/docs/3.5.1/api/java/org/apache/spark/sql/functions.html
 */
public class Coalesce {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Spark Create Map Column").master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		// input path : data/green_tripdata.parquet
		Dataset<Row> df = spark.read().parquet(args[0]);
		df.printSchema();
		df.show();
		
		Dataset<Row> withColumn = df.withColumn("COALESCE", functions.coalesce(df.col("ehail_fee"),df.col("extra")));
		
		// Returns the first column that is not null, or null if all inputs are null.
		withColumn.select("COALESCE","ehail_fee","extra").show();

	}
}
