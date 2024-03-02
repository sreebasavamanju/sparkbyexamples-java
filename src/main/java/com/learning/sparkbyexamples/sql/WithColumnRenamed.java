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

public class WithColumnRenamed {

	public static void main(String[] args) {

		// Create SparkSession
		SparkSession spark = SparkSession.builder().master("local[*]").appName("Spark With Column Renamed Examples")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		List<Row> rowsList = new ArrayList<>();
		rowsList.add(RowFactory.create(RowFactory.create("James", "", "Smith"), "36636", "M", 3000));
		rowsList.add(RowFactory.create(RowFactory.create("Anna", "Rose", ""), "40288", "M", 4000));
		rowsList.add(RowFactory.create(RowFactory.create("Julia", "", "Williams"), "42114", "M", 4000));
		rowsList.add(RowFactory.create(RowFactory.create("Maria", "Anne", "Jones"), "39192", "F", 4000));
		rowsList.add(RowFactory.create(RowFactory.create("Jen", "Mary", "Brown"), "", "F", -1));

		// Build Schema
		StructType schema = new StructType()
				.add("name",
						new StructType().add("firstname", DataTypes.StringType).add("middlename", DataTypes.StringType)
								.add("lastname", DataTypes.StringType))
				.add("dob", DataTypes.StringType).add("gender", DataTypes.StringType)
				.add("salary", DataTypes.IntegerType);

		Dataset<Row> df = spark.createDataFrame(rowsList, schema);

		df.printSchema();
		df.show(false);

		// Using Spark withColumnRenamed – To rename DataFrame column name
		df.withColumnRenamed("dob", "DateOfBirth").printSchema();

		// Using Spark StructType – To rename a nested column in Dataframe
		StructType schemaNew = new StructType().add("fname", DataTypes.StringType)
				.add("middlename", DataTypes.StringType).add("lname", DataTypes.StringType);

		df.select(col("name").cast(schemaNew), col("dob"), col("gender"), col("salary")).printSchema();

		// Using Select – To rename nested elements.
		// Let’s see another way to change nested columns by transposing the structure
		// to flat.
		df.select(col("name.firstname").as("fname"), col("name.middlename").as("mname"),
				col("name.lastname").as("lname"), col("dob"), col("gender"), col("salary")).printSchema();
		// Using Spark DataFrame withColumn – To rename nested columns

		df.withColumn("fname", col("name.firstname")).withColumn("mname", col("name.middlename"))
				.withColumn("lname", col("name.lastname")).drop("name").printSchema();
	}
}
