package com.learning.sparkbyexamples.problems;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class ProblemFive {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().appName("Problems and Solution").master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		StructType schema = new StructType().add("customer_id", DataTypes.IntegerType)
				.add("brand_name", DataTypes.StringType).add("prod_name", DataTypes.StringType);

		List<Row> values = new ArrayList<>();
		values.add(RowFactory.create(1, "Parle", "Parle-G"));
		values.add(RowFactory.create(1, "Britannia", "Marie Gold"));
		values.add(RowFactory.create(1, "ITC", "Marie Lite"));
		values.add(RowFactory.create(1, "Anmol", "Dream Lite"));
		values.add(RowFactory.create(2, "Unibic", "Choco-chip Cookies"));
		values.add(RowFactory.create(2, "Unibic", "Butter Cookies"));
		values.add(RowFactory.create(3, "Cremica", "Jira Lite"));
		values.add(RowFactory.create(4, "ITC", "Marie Lite"));
		values.add(RowFactory.create(4, "Britannia", "Marie Gold"));
		values.add(RowFactory.create(5, "Unibic", "Cashew Cookies"));
		values.add(RowFactory.create(5, "Unibic", "Butter Cookies"));
		values.add(RowFactory.create(5, "Unibic", "Fruit & nut Cookies"));

		Dataset<Row> df = spark.createDataFrame(values, schema);
		df.show();
		df.printSchema();
		df.cache();
		/*
		 * 1. Filter: Customers with more than one product
		 */

		Dataset<Row> groupByDf = df.groupBy(df.col("customer_id"))
				.agg(functions.count(df.col("prod_name")).as("prod_count"));
		groupByDf.filter(groupByDf.col("prod_count").gt(1)).orderBy(groupByDf.col("prod_count").desc()).show();

		/*
		 * 2.Rank products by customer_id using dense_rank
		 */
		WindowSpec partitionBy = Window.partitionBy(df.col("customer_id")).orderBy(df.col("prod_name"));
		Dataset<Row> withColumn = df.withColumn("dense_rank", functions.dense_rank().over(partitionBy));
		withColumn.show();

		/*
		 * 3. Group by brand and count products
		 */
		df.groupBy(df.col("brand_name")).agg(functions.count(df.col("prod_name"))).show();
	}
}
