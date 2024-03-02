package com.learning.sparkbyexamples.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DataFrameCreation {

	public static void main(String[] args) {

		// Create SparkSession
		SparkSession spark = SparkSession.builder().master("local[*]").appName("Different Ways to create Dataframe")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		// Create Data
		List<Row> rowList = new ArrayList<>();
		rowList.add(RowFactory.create("Java", "20000"));
		rowList.add(RowFactory.create("Python", "10000"));
		rowList.add(RowFactory.create("Scala", "3000"));

		StructField[] fieldsArray = new StructField[2];
		fieldsArray[0] = new StructField("language", DataTypes.StringType, true, Metadata.empty());

		fieldsArray[1] = new StructField("usersCount", DataTypes.StringType, true, Metadata.empty());

		StructType schema = new StructType(fieldsArray);

		// Creating a Dataframe from ROW alogn with schema
		Dataset<Row> df = spark.createDataFrame(rowList, schema);

		df.show();

		/*
		 * // create a Dataframe from csv
		 * 
		 * Dataset<Row> csvDF = spark.read().csv("/resources/file.csv"); csvDF.show();
		 * 
		 * 
		 * // creating a Dataframe from text file
		 * 
		 * Dataset<Row> textDF = spark.read().csv("/resources/file.csv"); textDF.show();
		 */
	}
}
