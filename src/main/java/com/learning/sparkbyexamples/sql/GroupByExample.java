package com.learning.sparkbyexamples.sql;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.sum;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * 
 * When we perform groupBy() on Spark Dataframe, it returns
 * RelationalGroupedDataset object which contains below aggregate functions.
 * 
 * count() - Returns the count of rows for each group.
 * 
 * mean() - Returns the mean of values for each group.
 * 
 * max() - Returns the maximum of values for each group.
 * 
 * min() - Returns the minimum of values for each group.
 * 
 * sum() - Returns the total for values for each group.
 * 
 * avg() - Returns the average for values for each group.
 * 
 * agg() - Using agg() function, we can calculate more than one aggregate at a
 * time.
 * 
 * pivot() - This function is used to Pivot the DataFrame which I will not be
 * covered in this.
 *
 */
public class GroupByExample {

	public static void main(String[] args) {
		// create SparkSession
		SparkSession spark = SparkSession.builder().master("local[*]").appName("GroupByExamples-Spark").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		// create the sample data
		List<Row> rowList = new ArrayList<>();
		rowList.add(RowFactory.create("James", "Sales", "NY", 90000, 34, 10000));
		rowList.add(RowFactory.create("Michael", "Sales", "NY", 86000, 56, 20000));
		rowList.add(RowFactory.create("Robert", "Sales", "CA", 81000, 30, 23000));
		rowList.add(RowFactory.create("Maria", "Finance", "CA", 90000, 24, 23000));
		rowList.add(RowFactory.create("Raman", "Finance", "CA", 99000, 40, 24000));
		rowList.add(RowFactory.create("Scott", "Finance", "NY", 83000, 36, 19000));
		rowList.add(RowFactory.create("Jen", "Finance", "NY", 79000, 53, 15000));
		rowList.add(RowFactory.create("Jeff", "Marketing", "CA", 80000, 25, 18000));
		rowList.add(RowFactory.create("Kumar", "Marketing", "NY", 91000, 50, 21000));

		StructType schema = new StructType().add("employee_name", DataTypes.StringType)
				.add("department", DataTypes.StringType).add("state", DataTypes.StringType)
				.add("salary", DataTypes.IntegerType).add("age", DataTypes.IntegerType)
				.add("bonus", DataTypes.IntegerType);

		Dataset<Row> df = spark.createDataFrame(rowList, schema);
		df.show();

		// groupBy and aggregate on DataFrame columns

		System.out.println(
				"Let’s do the groupBy() on department column of DataFrame and then find the sum of salary for each department using sum() aggregate function.");

		df.groupBy(col("department")).sum("salary").show(false);

		System.out.println("======we can calculate the number of employee in each department using count()==========");
		df.groupBy(col("department")).count().show(false);

		System.out.println("Calculate the minimum salary of each department using min()");

		df.groupBy(col("department")).min("salary").show(false);

		System.out.println("Calculate the maximum salary of each department using max()");

		df.groupBy(col("department")).max("salary").show(false);

		System.out.println("Calculate the average salary of each department using avg()");

		df.groupBy(col("department")).avg("salary").show(false);

		System.out.println("Calculate the mean salary of each department using mean()\r\n");
		df.groupBy(col("department")).mean("salary").show(false);

		// groupBy and aggregate on multiple DataFrame columns

		System.out.println("groupBy and aggregate on multiple DataFrame columns");
		df.groupBy(col("department"), col("state")).sum("salary", "bonus").show(false);

		// Running more aggregates at a time

		System.out.println("===================Running more aggregates at a time=================");

		df.groupBy("department").agg(sum("salary").as("sum_salary"), avg("salary").as("avg_salary"),
				sum("bonus").as("sum_bonus"), max("bonus").as("max_bonus")).show(false);

		// Using filter on aggregate data
		/*
		 * Similar to SQL “HAVING” clause, On Spark DataFrame we can use either the
		 * where() or filter() function to filter the rows of aggregated data.
		 */

		df.groupBy("department").agg(sum("salary").as("sum_salary"), avg("salary").as("avg_salary"),
				sum("bonus").as("sum_bonus"), max("bonus").as("max_bonus")).where(col("sum_bonus").geq(50000))
				.show(false);

	}
}
