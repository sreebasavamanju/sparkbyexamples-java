package com.learning.sparkbyexamples.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * https://sparkbyexamples.com/spark/how-to-pivot-table-and-unpivot-a-spark-dataframe/
 */
public class PivotUnpivotExample {

	public static void main(String[] args) {
		// create a sparksession
		SparkSession spark = SparkSession.builder().master("local[*]").appName("Spark pivot and unpivot Example")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		// creata a DataFrame
		List<Row> rowsList = new ArrayList<>();
		rowsList.add(RowFactory.create("Orange", 2000, "USA"));
		rowsList.add(RowFactory.create("Orange", 2000, "USA"));
		rowsList.add(RowFactory.create("Banana", 400, "China"));
		rowsList.add(RowFactory.create("Banana", 1000, "USA"));
		rowsList.add(RowFactory.create("Carrots", 1500, "USA"));
		rowsList.add(RowFactory.create("Beans", 1600, "USA"));
		rowsList.add(RowFactory.create("Carrots", 1200, "China"));
		rowsList.add(RowFactory.create("Beans", 1500, "China"));
		rowsList.add(RowFactory.create("Orange", 4000, "China"));
		rowsList.add(RowFactory.create("Banana", 2000, "Canada"));
		rowsList.add(RowFactory.create("Carrots", 2000, "Canada"));
		rowsList.add(RowFactory.create("Beans", 2000, "Mexico"));

		StructType schmea = new StructType().add("Product", DataTypes.StringType).add("Amount", DataTypes.IntegerType)
				.add("Country", DataTypes.StringType);

		Dataset<Row> df = spark.createDataFrame(rowsList, schmea);

		df.printSchema();
		df.show(false);

		// Pivot Spark DataFrame
		/**
		 * Spark SQL provides pivot() function to rotate the data from one column into
		 * multiple columns (transpose row to column). It is an aggregation where one of
		 * the grouping columns values transposed into individual columns with distinct
		 * data. From the above DataFrame, to get the total amount exported to each
		 * country of each product will do group by Product, pivot by Country, and the
		 * sum of Amount.
		 */

		System.out.println("===================Pivot Country column===============");
		Dataset<Row> pivotDF = df.groupBy(functions.col("Product")).pivot("Country").sum("Amount");
		pivotDF.printSchema();
		pivotDF.show();

		// Unpivot Spark DataFrame
		/**
		 * Unpivot is a reverse operation, we can achieve by rotating column values into
		 * rows values. Spark SQL doesn’t have unpivot function hence will use the
		 * stack() function. Below code converts column countries to row.
		 */

		
		System.out.println("===================UNPivot Country column===============");
		Dataset<Row> unPivotDF = pivotDF
				.select(functions.col("Product"),
						functions.expr(
								"stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"))
				.where("Total is not null");
		unPivotDF.printSchema();
		unPivotDF.show();
	}
}
