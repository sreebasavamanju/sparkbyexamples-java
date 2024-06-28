package com.learning.sparkbyexamples.sql.functions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Array {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Spark Create Array - Example").master("local[*]")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		// input path : data/green_tripdata.parquet
		Dataset<Row> df = spark.read().parquet(args[0]);
		df.printSchema();
		df.show();

		// We can create array from existing columns with same datatype
		df.withColumn("Array_COL", functions.array(df.col("trip_distance"), df.col("fare_amount"), df.col("extra")))
				.select("Array_COL", "trip_distance", "fare_amount", "extra").show();

		// another method signuture
		df.withColumn("Array_COL", functions.array("trip_distance", "fare_amount", "extra"))
				.select("Array_COL", "trip_distance", "fare_amount", "extra").show();

		// What if we try to mix datatype while creating array if any of the column is
		// string then it will convert all of them to string it wont throw error here
		Dataset<Row> select = df.withColumn("Array_COL",
				functions.array(df.col("trip_distance"), df.col("fare_amount"), df.col("extra"),
						df.col("store_and_fwd_flag"), df.col("lpep_dropoff_datetime")))
				.select("Array_COL", "trip_distance", "fare_amount", "extra", "store_and_fwd_flag",
						"lpep_dropoff_datetime");
		select.schema().printTreeString();
		select.show(false);

		// What if we try to mix datatype while creating array if any of the column is
		// string then it will convert all of them to string, error will be thrown here
		select = df
				.withColumn("Array_COL",
						functions.array(df.col("trip_distance"), df.col("fare_amount"), df.col("extra"),
								df.col("lpep_dropoff_datetime")))
				.select("Array_COL", "trip_distance", "fare_amount", "extra", "lpep_dropoff_datetime");
		select.schema().printTreeString();
		select.show(false);
	}
}
