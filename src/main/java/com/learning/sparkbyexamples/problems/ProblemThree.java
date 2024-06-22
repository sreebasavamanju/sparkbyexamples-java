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

public class ProblemThree {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Problemns and Solutions").master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		/*
		 * -----------------------------Problem statement ----------------------------
		 * You are given transaction dataframe where we need to identify returning
		 * active users.Returning active user is the user who made a transaction within
		 * 7 days of their first purchase.
		 */

		List<Row> transctionsList = new ArrayList<>();
		transctionsList.add(RowFactory.create(1, 101, "2023-05-01"));
		transctionsList.add(RowFactory.create(2, 101, "2023-05-05"));
		transctionsList.add(RowFactory.create(3, 102, "2023-05-01"));
		transctionsList.add(RowFactory.create(4, 103, "2023-05-01"));
		transctionsList.add(RowFactory.create(5, 101, "2023-05-12"));
		transctionsList.add(RowFactory.create(6, 102, "2023-05-10"));
		transctionsList.add(RowFactory.create(7, 103, "2023-05-08"));
		transctionsList.add(RowFactory.create(8, 104, "2023-05-01"));
		transctionsList.add(RowFactory.create(9, 104, "2023-05-08"));
		transctionsList.add(RowFactory.create(10, 105, "2023-05-10"));

		StructType schema = new StructType().add("Id", DataTypes.IntegerType).add("user_id", DataTypes.IntegerType)
				.add("date", DataTypes.StringType);
		Dataset<Row> df = spark.createDataFrame(transctionsList, schema);
		df = df.withColumn("date", functions.to_date(df.col("date")));
		df.show();
		df.printSchema();

		WindowSpec spec = Window.partitionBy(df.col("user_id")).orderBy(df.col("date"));

		// Create rank among the user tractions to find first two
		df = df.withColumn("rank", functions.rank().over(spec));
		df.show();

		Dataset<Row> c1 = df.alias("c1");
		Dataset<Row> c2 = df.alias("c2");

		c1.join(c2,
				c1.col("user_Id").equalTo(c2.col("user_Id")).and(functions.col("c1.rank").equalTo(1))
						.and(functions.col("c2.rank").equalTo(2))
						.and(functions.date_diff(functions.col("c2.date"), functions.col("c1.date")).leq(7)))
				.select(functions.col("c1.id"), functions.col("c1.date").as("first_date"),
						functions.col("c2.date").as("second_date"))
				.show();
	}
}
