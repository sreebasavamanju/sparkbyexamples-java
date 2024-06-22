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

public class ProblemFour {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Problems and Solution Weather Data").master("local[*]")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		/*
		 * You are given a weather dataframe. id is the column with unique values for
		 * this dataframe. There are no different rows with the same recordDate. This
		 * table contains information about the temperature on a certain day.
		 * 
		 * Write a solution to find all dates' Id with higher temperatures compared to
		 * its previous dates (yesterday). Return the result table in any order.
		 */

		List<Row> weatherList = new ArrayList<>();
		weatherList.add(RowFactory.create(1,"2015-01-01",10));
		weatherList.add(RowFactory.create(2,"2015-01-02",25));
		weatherList.add(RowFactory.create(3,"2015-01-03",20));
		weatherList.add(RowFactory.create(4,"2015-01-04",30));
		weatherList.add(RowFactory.create(5,"2015-01-07",40));
		weatherList.add(RowFactory.create(6,"2015-01-09",50));
		
		StructType schema = new StructType().add("id",DataTypes.IntegerType).add("recordDate",DataTypes.StringType).add("temperature",DataTypes.IntegerType);
		
		Dataset<Row> df = spark.createDataFrame(weatherList, schema);
		df=df.withColumn("recordDate", functions.to_date(df.col("recordDate")));
		df.show();
		df.printSchema();
		
		WindowSpec windowSpec = Window.orderBy(df.col("recordDate"));
		df=df.withColumn("prev", functions.lag(df.col("temperature"),1).over(windowSpec));
		df.show();
		df=df.withColumn("dateDiff", functions.date_diff(df.col("recordDate"),functions.lag(df.col("recordDate"),1).over(windowSpec)));
		df.show();
		
		df.filter(df.col("temperature").gt(df.col("prev")).and(df.col("dateDiff").equalTo(1))).show();

	}
}
