package com.learning.sparkbyexamples.sql.functions;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Map {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Spark Create Map Column").master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		// input path : data/green_tripdata.parquet
		Dataset<Row> df = spark.read().parquet(args[0]);
		df.printSchema();
		df.show();
		Column[] columns = new Column[df.columns().length * 2];
		int index = 0;
		for (String colName : df.columns()) {
			columns[index++] = functions.lit(colName);
			columns[index++] = functions.col(colName);
		}

		df = df.withColumn("Map_COL", functions.map(columns)).select("Map_COL");
		df.show(false);

		df.select("Map_COL.VendorID").show();

	}

}
