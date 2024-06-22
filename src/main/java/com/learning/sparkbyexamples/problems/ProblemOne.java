package com.learning.sparkbyexamples.problems;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class ProblemOne {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Problems And Solutions").master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		// Question 1:
		/*
		 * You have a DataFrame containing information about products, including their
		 * names and prices. You are tasked with creating a new column, "PriceCategory,"
		 * based on the following conditions: If the price is less than 50, categorize
		 * it as "Low." If the price is between 50 (inclusive) and 100 (exclusive),
		 * categorize it as "Medium." If the price is 100 or greater, categorize it as
		 * "High."
		 */
		List<Row> rowList = new ArrayList<>();
		rowList.add(RowFactory.create("ProduictA", 30));
		rowList.add(RowFactory.create("ProduictB", 90));
		rowList.add(RowFactory.create("ProduictC", 100));

		StructType schema = new StructType().add("ProductName", DataTypes.StringType).add("Price",
				DataTypes.IntegerType);

		Dataset<Row> df = spark.createDataFrame(rowList, schema);
		df.printSchema();
		df.show();
		df.createOrReplaceTempView("products");
		df = df.withColumn("PriceCategory",
				functions.when(df.col("Price").geq(100), functions.lit("High"))
						.when(df.col("Price").lt(100).and(df.col("Price").gt(50)), functions.lit("Medium"))
						.otherwise(functions.lit("Low")));
		df.show();
		// SQL 
		spark.sql("SELECT *, CASE WHEN Price >= 100 THEN 'HIGH'"
				+ "WHEN Price >=50 AND Price <100 THEN 'Medium'"
				+ "ELSE 'LOW' END AS ProductCategory FROM products").show();

	}
}
