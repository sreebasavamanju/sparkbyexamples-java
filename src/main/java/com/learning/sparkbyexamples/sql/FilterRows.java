package com.learning.sparkbyexamples.sql;

import static org.apache.spark.sql.functions.array_contains;
import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class FilterRows {

	public static void main(String[] args) {
		// Create SparkSession
		SparkSession spark = SparkSession.builder().master("local[*]").appName("Filter Rows").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		// create a DataFrame first

		List<Row> rowsList = new ArrayList<>();
		rowsList.add(
				RowFactory.create(RowFactory.create("James", "", "Smith"), List.of("Java", "Scala", "C++"), "OH", "M"));
		rowsList.add(
				RowFactory.create(RowFactory.create("Anna", "Rose", ""), List.of("Spark", "Java", "C++"), "NY", "F"));
		rowsList.add(RowFactory.create(RowFactory.create("Julia", "", "Williams"), List.of("CSharp", "VB"), "OH", "F"));
		rowsList.add(
				RowFactory.create(RowFactory.create("Maria", "Anne", "Jones"), List.of("CSharp", "VB"), "NY", "M"));
		rowsList.add(RowFactory.create(RowFactory.create("Jen", "Mary", "Brown"), List.of("CSharp", "VB"), "NY", "M"));
		rowsList.add(
				RowFactory.create(RowFactory.create("Mike", "Mary", "Williams"), List.of("Python", "VB"), "OH", "M"));

		// Build Schema
		StructType schema = new StructType()
				.add("name",
						new StructType().add("firstname", DataTypes.StringType).add("middlename", DataTypes.StringType)
								.add("lastname", DataTypes.StringType))
				.add("languages", DataTypes.createArrayType(DataTypes.StringType)).add("state", DataTypes.StringType)
				.add("gender", DataTypes.StringType);

		Dataset<Row> df = spark.createDataFrame(rowsList, schema);

		df.printSchema();
		df.show(false);

		System.out.println("============Filter rows with state as 'OH'=============== ");
		df.where(col("state").equalTo("OH")).show(false);

		System.out.println("============Filter rows with gender as 'M'=============== ");
		// DataFrame where() with SQL Expression
		df.where("gender == 'M'").show(false);

		System.out.println(
				"============Filter rows with multiple condition state as 'OH' and  gender as 'M' =============== ");
		// Multiple condition

		df.where(col("state").equalTo("OH").and(col("gender").equalTo("M"))).show(false);

		System.out.println(
				"============Filter rows on arrays column languages which contains java as an array element' =============== ");

		// Filtering on an Array column
		df.where(array_contains(col("languages"), "Java")).show(false);

		// Filtering on Nested Struct columns
		// Struct condition
		System.out
				.println("============Filter rows on struct nested column name.lastname is Williams =============== ");

		df.where(col("name.lastname").equalTo("Williams")).show(false);

		// Alternatively, you also use filter() function to filter the rows on
		// DataFrame.

	}
}
