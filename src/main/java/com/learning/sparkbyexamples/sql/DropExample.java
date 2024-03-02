package com.learning.sparkbyexamples.sql;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class DropExample {

	public static void main(String[] args) {
		// Create SparkSession
		SparkSession spark = SparkSession.builder().master("local[*]").appName("Spark With Column Renamed Examples")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		List<Row> rowList = new ArrayList<>();
		rowList.add(RowFactory.create("James", "", "Smith", "36636", "NewYork", 3100));
		rowList.add(RowFactory.create("Michael", "Rose", "", "40288", "California", 4300));
		rowList.add(RowFactory.create("Robert", "", "Williams", "42114", "Florida", 1400));
		rowList.add(RowFactory.create("Maria", "Anne", "Jones", "39192", "Florida", 5500));
		rowList.add(RowFactory.create("Jen", "Mary", "Brown", "34561", "NewYork", 3000));

		StructType structureSchema = new StructType().add("firstname", DataTypes.StringType)
				.add("middlename", DataTypes.StringType).add("lastname", DataTypes.StringType)
				.add("id", DataTypes.StringType).add("location", DataTypes.StringType)
				.add("salary", DataTypes.IntegerType);

		// create DataFrame
		Dataset<Row> df = spark.createDataFrame(rowList, structureSchema);

		df.printSchema();

		// Drop one column from DataFrame

		// Drop one column from DataFrame
		var df2 = df.drop("firstname"); // First signature
		df2.printSchema();

		df.drop(df.col("firstname")).printSchema();

		// Import org.apache.spark.sql.functions.col is required
		df.drop(col("firstname")).printSchema();// Third signature

		// Drop multiple columns from DataFrame
		System.out.println("============Drop multiple columns from DataFrame====================");
		df.drop("firstname", "middlename", "lastname").printSchema();

	}
}
