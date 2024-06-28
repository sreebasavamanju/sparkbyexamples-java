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

public class SparkGroupByCollectList {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Spark groupby column and collect as list")
				.master("local[*]").getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");

		List<Row> rowList = new ArrayList<>();
		rowList.add(RowFactory.create("a", "aa", 1));
		rowList.add(RowFactory.create("a", "aa", 2));
		rowList.add(RowFactory.create("b", "ab", 3));
		rowList.add(RowFactory.create("b", "ab", 4));
		rowList.add(RowFactory.create("b", "ab", 5));
		rowList.add(RowFactory.create("b", "ac", 6));

		StructType schema = new StructType().add("col1", DataTypes.StringType).add("col2", DataTypes.StringType)
				.add("col3", DataTypes.IntegerType);

		Dataset<Row> df = spark.createDataFrame(rowList, schema);

		df.show();

		df.groupBy("col1", "col2").agg(functions.collect_list("col3").as("col3")).show();

	}
}
