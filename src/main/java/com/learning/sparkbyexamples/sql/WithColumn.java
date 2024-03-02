package com.learning.sparkbyexamples.sql;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class WithColumn {

	public static void main(String[] args) {
		// Create SparkSession

		SparkSession spark = SparkSession.builder().appName("Spark WihColumn Examples").master("local[*]")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		// create a DataFrame first

		List<Row> rowsList = new ArrayList<>();
		rowsList.add(RowFactory.create(RowFactory.create("James", "", "Smith"), "36636", "M", "3000"));
		rowsList.add(RowFactory.create(RowFactory.create("Anna", "Rose", ""), "40288", "M", "4000"));
		rowsList.add(RowFactory.create(RowFactory.create("Julia", "", "Williams"), "42114", "M", "4000"));
		rowsList.add(RowFactory.create(RowFactory.create("Maria", "Anne", "Jones"), "39192", "F", "4000"));
		rowsList.add(RowFactory.create(RowFactory.create("Jen", "Mary", "Brown"), "", "F", "-1"));

		// Build Schema
		StructType schema = new StructType()
				.add("name",
						new StructType().add("firstname", DataTypes.StringType).add("middlename", DataTypes.StringType)
								.add("lastname", DataTypes.StringType))
				.add("dob", DataTypes.StringType).add("gender", DataTypes.StringType)
				.add("salary", DataTypes.StringType);

		Dataset<Row> df = spark.createDataFrame(rowsList, schema);

		df.printSchema();
		df.show(false);

		// Add a New Column to DataFrame
		System.out.println("============Adding a new column with constant value=============== ");
		df.withColumn("Country", lit("USA")).show(false);

		// Change Value of an Existing Column
		System.out
				.println("============Change Value of an Existing Column, increasing salary 100 times=============== ");

		df.withColumn("salary", col("salary").multiply(100)).show(false);

		// the above code multiplies the value of “salary” with 100 and updates the
		// value back to “salary” column.

		// Change Value of an Existing Column
		System.out.println("==============Derive New Column From an Existing Column============= ");
		// Derive New Column From an Existing Column
		df = df.withColumn("CopiedColumn", col("salary").multiply(-1));
		df.show(false);

		// Change the column data type
		System.out.println("============Change the column(salary) data type from String to Integer=============== ");
		df.withColumn("salary", col("salary").cast(DataTypes.IntegerType)).printSchema();

		// Rename Column Name
		System.out.println(
				"============To rename an existing column use “withColumnRenamed” function on DataFrame.=========");
		df.withColumnRenamed("gender", "sex").show(false);

		// Drop a column

		System.out.println(
				"==============Use drop() function to drop a specific column from the DataFrame.=============");
		df.drop(col("CopiedColumn")).show(false);

		// Split Column into multiple Columns

		System.out.println("==============Split Column into multiple Columns=============");

		/**
		 * Though this example doesn’t use withColumn() function, I still feel like it’s
		 * good to explain on splitting one DataFrame column to multiple columns using
		 * Spark map() transformation function.
		 */

		StructType columns = new StructType().add("name", DataTypes.StringType).add("address", DataTypes.StringType);
		List<Row> data = Arrays.asList(RowFactory.create("Robert, Smith", "1 Main st, Newark, NJ, 92537"),
				RowFactory.create("Maria, Garcia", "3456 Walnut st, Newark, NJ, 94732"));

		Dataset<Row> dfFromData = spark.createDataFrame(data, columns);
		dfFromData.printSchema();
		
		dfFromData.show(false);

		StructType newColumns = new StructType().add("Firstname", DataTypes.StringType)
				.add("Lastname", DataTypes.StringType).add("AddressLine1", DataTypes.StringType)
				.add("City", DataTypes.StringType).add("State", DataTypes.StringType)
				.add("ZipCode", DataTypes.StringType);

		Dataset<Row> newDF = dfFromData.map((MapFunction<Row,Row>)f -> {
			String[] nameSplit = f.getString(0).split(",");
			String[] addressSplit = f.getString(1).split(",");
			return RowFactory.create(nameSplit[0], nameSplit[1], addressSplit[0], addressSplit[1], addressSplit[2],
					addressSplit[3]);
		},Encoders.row(newColumns));
		newDF.printSchema();
		newDF.show(false);

	}
}
