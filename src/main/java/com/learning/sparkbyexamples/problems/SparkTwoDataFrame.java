package com.learning.sparkbyexamples.problems;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.learning.sparkbyexamples.algorithms.Utils;

/**
 * https://stackoverflow.com/questions/69886683/pyspark-difference-between-2-dataframes-identify-inserts-updates-and-delete
 * 
 * Difference between 2 dataframes - Identify inserts, updates and deletes
 */
public class SparkTwoDataFrame {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName("Spark -Different betweeon two DF and update status as Insert,Update,Delete")
				.master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		List<Row> prevDayRowList = new ArrayList<>();
		prevDayRowList.add(RowFactory.create("James", 36636, "M", 3000));
		prevDayRowList.add(RowFactory.create("Michael", 40288, "M", 4000));
		prevDayRowList.add(RowFactory.create("Robert", 42114, "M", 4000));
		prevDayRowList.add(RowFactory.create("Maria", 39192, "F", 4000));
		prevDayRowList.add(RowFactory.create("Jen", 60563, "F", -1));

		List<Row> currentDayRowList = new ArrayList<>();
		currentDayRowList.add(RowFactory.create("James", 36636, "M", 3000));
		currentDayRowList.add(RowFactory.create("Robert", 42114, "M", 2000));
		currentDayRowList.add(RowFactory.create("Maria", 72712, "F", 3000));
		currentDayRowList.add(RowFactory.create("Yesh", 75234, "M", 3000));
		currentDayRowList.add(RowFactory.create("Jen", 60563, "F", -1));

		StructType schema = new StructType().add("firstname", DataTypes.StringType).add("id", DataTypes.IntegerType)
				.add("gender", DataTypes.StringType).add("salary", DataTypes.IntegerType);

		
		Dataset<Row> prevDF = spark.createDataFrame(prevDayRowList, schema);
		Dataset<Row> currentDF = spark.createDataFrame(currentDayRowList, schema);
		Dataset<Row> join = prevDF.join(currentDF, prevDF.col("id").equalTo(currentDF.col("id")), "outer");

		Dataset<Row> withColumn = join.withColumn("status",
				functions.when(prevDF.col("id").isNull(), functions.lit("added"))
						.when(currentDF.col("id").isNull(), functions.lit("deleted"))
						.when(prevDF.col("firstname").equalTo(currentDF.col("firstname"))
								.and(prevDF.col("id").equalTo(currentDF.col("id")))
								.and(prevDF.col("gender").equalTo(currentDF.col("gender")))
								.and(prevDF.col("salary").equalTo(currentDF.col("salary"))), "unchanged")
						.otherwise("change"));
		Dataset<Row> select = withColumn.select(functions.coalesce(currentDF.col("firstname"), prevDF.col("firstname")).as("firstname"),
				functions.coalesce(currentDF.col("id"), prevDF.col("id")).as("id"),
				functions.coalesce(currentDF.col("gender"), prevDF.col("gender")).as("gender"),
				functions.coalesce(currentDF.col("salary"), prevDF.col("salary")).as("salary"), functions.col("status"))
				;
		select.explain();
		select.show();

		Utils.waitForExit();

	}
}
