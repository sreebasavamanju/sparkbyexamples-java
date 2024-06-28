package com.learning.sparkbyexamples.problems;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkStockAvg {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Spark Stock Avg Price in a day and max price among stocks")
				.master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		List<Row> rowList = new ArrayList<>();
		rowList.add(RowFactory.create("2023-01-01", "RIL", 150.00));
		rowList.add(RowFactory.create("2023-01-01", "RIL", 175.00));
		rowList.add(RowFactory.create("2023-01-01", "HDFC", 178.00));
		rowList.add(RowFactory.create("2023-01-02", "HDFC", 150.00));
		rowList.add(RowFactory.create("2023-01-02", "ITC", 200.00));
		rowList.add(RowFactory.create("2023-01-03", "ITC", 400.00));
		rowList.add(RowFactory.create("2023-01-03", "ITC", 420.00));
		rowList.add(RowFactory.create("2023-01-03", "RIL", 2000.00));
		rowList.add(RowFactory.create("2023-01-03", "HDFC", 1100.00));
		rowList.add(RowFactory.create("2023-01-03", "HDFC", 1200.00));

		StructType schema = new StructType().add("date", DataTypes.StringType).add("symbol", DataTypes.StringType)
				.add("price", DataTypes.DoubleType);

		Dataset<Row> df = spark.createDataFrame(rowList, schema);
		df.show();

		df = df.withColumn("date", functions.to_date(df.col("date")));

		Dataset<Row> aggDF = df.groupBy(df.col("date"), df.col("symbol")).agg(functions.avg("price").as("avg_price"));

		aggDF.show();

		aggDF.groupBy("symbol").agg(functions.max(aggDF.col("avg_price")).as("max_avg_price_per_share")).show();

	}
}
