package com.learning.sparkbyexamples.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * https://sparkbyexamples.com/spark/spark-dataframe-union-and-union-all/
 */
public class UnionExample {

	public static void main(String[] args) {
		// create SparkSession

		SparkSession spark = SparkSession.builder().master("local[*]").appName("Spark Union and UnionALL Example")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		// create a First DataFrame

		List<Row> rowsList = new ArrayList<>();
		rowsList.add(RowFactory.create("James", "Sales", "NY", 90000, 34, 10000));
		rowsList.add(RowFactory.create("Michael", "Sales", "NY", 86000, 56, 20000));
		rowsList.add(RowFactory.create("Robert", "Sales", "CA", 81000, 30, 23000));
		rowsList.add(RowFactory.create("Maria", "Finance", "CA", 90000, 24, 23000));

		StructType schema = new StructType().add("employee_name", DataTypes.StringType)
				.add("department", DataTypes.StringType).add("state", DataTypes.StringType)
				.add("salary", DataTypes.IntegerType).add("age", DataTypes.IntegerType)
				.add("bonus", DataTypes.IntegerType);

		Dataset<Row> df = spark.createDataFrame(rowsList, schema);

		df.printSchema();
		df.show();

		// let’s create a second Dataframe

		List<Row> dataList = new ArrayList<Row>();
		dataList.add(RowFactory.create("James", "Sales", "NY", 90000, 34, 10000));
		dataList.add(RowFactory.create("Maria", "Finance", "CA", 90000, 24, 23000));
		dataList.add(RowFactory.create("Jen", "Finance", "NY", 79000, 53, 15000));
		dataList.add(RowFactory.create("Jeff", "Marketing", "CA", 80000, 25, 18000));
		dataList.add(RowFactory.create("Kumar", "Marketing", "NY", 91000, 50, 21000));

		Dataset<Row> df2 = spark.createDataFrame(dataList, schema);
		df2.printSchema();
		df2.show(false);

		// Combine two or more DataFrames using union
		/**
		 * DataFrame union() method combines two DataFrames and returns the new
		 * DataFrame with all rows from two Dataframes regardless of duplicate data.
		 */

		System.out.println("================UNION =========================");
		Dataset<Row> unionDF = df.union(df2);
		unionDF.show(false);

		// Combine DataFrames using unionAll
		/**
		 * DataFrame unionAll() method is deprecated since Spark “2.0.0” version and
		 * recommends using the union() method.
		 */
		System.out.println("====================UNIONALL - it will return output same as UNION===========");
		Dataset<Row> unionAll = df.unionAll(df2);
		unionAll.show(false);

		// Combine without Duplicates
		/**
		 * Since the union() method returns all rows without distinct records, we will
		 * use the distinct() function to return just one record when duplicate exists.
		 */
		System.out.println("==================== Combine without Duplicates with the help of DISTINCT========");
		Dataset<Row> distinctUnion = df.union(df2).distinct();
		distinctUnion.show(false);
	}
}
