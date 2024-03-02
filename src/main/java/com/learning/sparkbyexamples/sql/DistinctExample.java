package com.learning.sparkbyexamples.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class DistinctExample {

	public static void main(String[] args) {
		// Create SparkSession
		SparkSession spark = SparkSession.builder().master("local[*]").appName("Spark With Column Renamed Examples")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		List<Row> rowList = new ArrayList<>();
		rowList.add(RowFactory.create("James", "Sales", 3000));
		rowList.add(RowFactory.create("Michael", "Sales", 4600));
		rowList.add(RowFactory.create("Robert", "Sales", 4100));
		rowList.add(RowFactory.create("Maria", "Finance", 3000));
		rowList.add(RowFactory.create("James", "Sales", 3000));
		rowList.add(RowFactory.create("Scott", "Finance", 3300));
		rowList.add(RowFactory.create("Jen", "Finance", 3900));
		rowList.add(RowFactory.create("Jeff", "Marketing", 3000));
		rowList.add(RowFactory.create("Kumar", "Marketing", 2000));
		rowList.add(RowFactory.create("Saif", "Sales", 4100));

		StructType schema = new StructType().add("employee_name", DataTypes.StringType)
				.add("department", DataTypes.StringType).add("salary", DataTypes.IntegerType);

		Dataset<Row> df = spark.createDataFrame(rowList, schema);
		df.show();

		// Get Distinct All Columns

		System.out.println("========================Get Distinct All Columns=======================");

		/*
		 * distinct() function on DataFrame returns a new DataFrame after removing the
		 * duplicate records. This example yields the below output.
		 * 
		 */
		Dataset<Row> distinctDF = df.distinct();
		System.out.println("Distinct Count: " + distinctDF.count());
		distinctDF.show(false);

		/*
		 * Alternatively, you can also run dropDuplicates() function which returns a new
		 * DataFrame with duplicate rows removed.
		 */

		Dataset<Row> df2 = df.dropDuplicates();
		System.out.println("Distinct Count: " + df2.count());
		df2.show(false);

		// Spark Distinct of Multiple Columns
		/**
		 * Spark doesn’t have a distinct method that takes columns that should run
		 * distinct on however, Spark provides another signature of dropDuplicates()
		 * function which takes multiple columns to eliminate duplicates.
		 */

		Dataset<Row> dropDisDF = df.dropDuplicates("department", "salary");
		System.out.println("Distinct count of department & salary : " + dropDisDF.count());
		dropDisDF.show(false);
	}
}
