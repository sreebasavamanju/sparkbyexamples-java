package com.learning.sparkbyexamples.algorithms;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class LazyEvalution {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Lazy Evalution - Demo").master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("DEBUG");
		List<Row> rowList = new ArrayList<>();
		rowList.add(RowFactory.create("ALice", 34));
		rowList.add(RowFactory.create("Bob", 45));
		rowList.add(RowFactory.create("Charlie", 28));

		StructType schema = new StructType().add("name", DataTypes.StringType).add("age", DataTypes.IntegerType);

		Dataset<Row> df = spark.createDataFrame(rowList, schema);
		Dataset<Row> filter = df.filter(df.col("age").geq("30")).select(df.col("name"))
				.filter(df.col("name").startsWith(functions.lit("A")));
		filter.show();

		Utils.waitForExit();
	}
}
