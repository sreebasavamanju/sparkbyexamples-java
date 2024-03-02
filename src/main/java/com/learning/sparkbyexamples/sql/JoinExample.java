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

public class JoinExample {

	public static void main(String[] args) {
		// create SparkSession
		SparkSession spark = SparkSession.builder().master("local[*]").appName("Spark Joins Example").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		List<Row> empRowList = new ArrayList<>();
		empRowList.add(RowFactory.create(1, "Smith", -1, "2018", "10", "M", 3000));
		empRowList.add(RowFactory.create(2, "Rose", 1, "2010", "20", "M", 4000));
		empRowList.add(RowFactory.create(3, "Williams", 1, "2010", "10", "M", 1000));
		empRowList.add(RowFactory.create(4, "Jones", 2, "2005", "10", "F", 2000));
		empRowList.add(RowFactory.create(5, "Brown", 2, "2010", "40", "", -1));
		empRowList.add(RowFactory.create(6, "Brown", 2, "2010", "50", "", -1));

		StructType empSchema = new StructType().add("emp_id", DataTypes.IntegerType).add("name", DataTypes.StringType)
				.add("superior_emp_id", DataTypes.IntegerType).add("year_joined", DataTypes.StringType)
				.add("emp_dept_id", DataTypes.StringType).add("gender", DataTypes.StringType)
				.add("salary", DataTypes.IntegerType);

		Dataset<Row> empDF = spark.createDataFrame(empRowList, empSchema);

		empDF.show(false);

		List<Row> deptRowList = new ArrayList<>();
		deptRowList.add(RowFactory.create("Finance", 10));
		deptRowList.add(RowFactory.create("Marketing", 20));
		deptRowList.add(RowFactory.create("Sales", 30));
		deptRowList.add(RowFactory.create("IT", 40));

		StructType deptSchema = new StructType().add("dept_name", DataTypes.StringType).add("dept_id",
				DataTypes.IntegerType);

		Dataset<Row> deptDF = spark.createDataFrame(deptRowList, deptSchema);

		deptDF.show();

		// Inner Join
		/*
		 * Spark Inner join is the default join and it’s mostly used, It is used to join
		 * two DataFrames/Datasets on key columns, and where keys don’t match the rows
		 * get dropped from both datasets (emp & dept).
		 */

		System.out.println("=============INNER JOIN================");
		empDF.join(deptDF, empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")), "inner").show(false);

		// Full Outer Join

		/**
		 * Outer a.k.a full, fullouter join returns all rows from both Spark
		 * DataFrame/Datasets, where join expression doesn’t match it returns null on
		 * respective record columns.
		 */

		System.out.println("=============Full/Outer/FullOuter JOIN================");
		empDF.join(deptDF, empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")), "outer").show(false);
		empDF.join(deptDF, empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")), "full").show(false);
		empDF.join(deptDF, empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")), "fullouter").show(false);

		// Left Outer Join
		/*
		 * Spark Left a.k.a Left Outer join returns all rows from the left
		 * DataFrame/Dataset regardless of match found on the right dataset when join
		 * expression doesn’t match, it assigns null for that record and drops records
		 * from right where match not found.
		 */

		System.out.println("=============left/leftOuter JOIN================");

		empDF.join(deptDF, empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")), "left").show(false);
		empDF.join(deptDF, empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")), "leftouter").show(false);

		System.out.println("============Return Only the unmatched row from the left table=======");
		// To return unmatched records from left table
		empDF.join(deptDF, empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")), "left")
				.filter(deptDF.col("dept_id").isNull()).show();

		// Right Outer Join
		/**
		 * Spark Right a.k.a Right Outer join is opposite of left join, here it returns
		 * all rows from the right DataFrame/Dataset regardless of math found on the
		 * left dataset, when join expression doesn’t match, it assigns null for that
		 * record and drops records from left where match not found.
		 */

		System.out.println("=============right/rightOuter JOIN================");
		empDF.join(deptDF, empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")), "right").show(false);
		empDF.join(deptDF, empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")), "rightouter").show(false);

		// Left Semi Join
		/**
		 * Spark Left Semi join is similar to inner join difference being leftsemi join
		 * returns all columns from the left DataFrame/Dataset and ignores all columns
		 * from the right dataset. In other words, this join returns columns from the
		 * only left dataset for the records match in the right dataset on join
		 * expression, records not matched on join expression are ignored from both left
		 * and right datasets.
		 */

		System.out.println("=============Left Semi  JOIN================");
		empDF.join(deptDF, empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")), "leftsemi").show(false);

		// Left Anti Join

		/**
		 * Left Anti join does the exact opposite of the Spark leftsemi join, leftanti
		 * join returns only columns from the left DataFrame/Dataset for non-matched
		 * records.
		 */
		System.out.println("===================Left Anti Join=========================");
		empDF.join(deptDF, empDF.col("emp_dept_id").equalTo(deptDF.col("dept_id")), "leftanti").show(false);

		// Self Join
		/**
		 * Spark Joins are not complete without a self join, Though there is no
		 * self-join type available, we can use any of the above-explained join types to
		 * join DataFrame to itself. below example use inner self join
		 */

		empDF.as("emp1").join(empDF.as("emp2"), col("emp1.superior_emp_id").equalTo(col("emp2.emp_id")), "inner")
				.select(col("emp1.emp_id"), col("emp1.name"), col("emp2.emp_id").as("superior_emp_id"),
						col("emp2.name").as("superior_emp_name"))
				.show(false);

		// Using SQL Expression

		empDF.createOrReplaceTempView("EMP");
		deptDF.createOrReplaceTempView("DEPT");
		// SQL JOIN
		Dataset<Row> joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id");
		joinDF.show(false);

		Dataset<Row> joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id");
		joinDF2.show(false);
	}
}
