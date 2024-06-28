package com.learning.sparkbyexamples.problems;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class DropDuplicates {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Spark remove Duplicates").master("local[*]").getOrCreate();
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

		prevDF.unionByName(currentDF).dropDuplicates().show();
	}
}
