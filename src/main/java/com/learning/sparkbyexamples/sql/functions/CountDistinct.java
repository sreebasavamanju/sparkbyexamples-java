package com.learning.sparkbyexamples.sql.functions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * https://spark.apache.org/docs/3.5.1/api/java/org/apache/spark/sql/functions.html
 * 
 * https://stackoverflow.com/questions/37949494/how-to-count-occurrences-of-each-distinct-value-for-every-column-in-a-dataframe
 */
public class CountDistinct {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Spark count Distinct - Example").master("local[*]")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		// input path : data/green_tripdata.parquet
		Dataset<Row> df = spark.read().parquet(args[0]);
		df.printSchema();
		df.show();
		df.cache();
		System.out.println("Total No of Rows : " + df.count());

		// Count Distinct
		df.select(functions.count_distinct(functions.col("PULocationID"))).show();

		df.select(functions.count_distinct(functions.col("PULocationID"), functions.col("DOLocationID"))).show();

		// same as above function just different name

		df.select(functions.countDistinct(functions.col("PULocationID"))).show();

		df.select(functions.countDistinct(functions.col("PULocationID"), functions.col("DOLocationID"))).show();
	}
}
