package com.learning.sparkbyexamples.problems.stackoverflow;

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
 * https://stackoverflow.com/questions/78654279/how-to-resolve-the-column-is-not-iterable-error-while-using-withcolumn-to-re
 */
public class SubstringToRemoveFirstCharacter {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Remove First Character from the columns")
				.master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		List<Row> rowList = new ArrayList<>();
		rowList.add(RowFactory.create("John", 26));
		rowList.add(RowFactory.create("Wick", 30));
		rowList.add(RowFactory.create("Arjunar", 35));

		StructType schema = new StructType().add("name", DataTypes.StringType).add("age", DataTypes.IntegerType);

		Dataset<Row> df = spark.createDataFrame(rowList, schema);
		df.show();
		// this wont work compilation error as the functions.substring expect both the
		// int numbers but the length of the column is of type column not int
		// df.withColumn("name", functions.substring(df.col("name"), 1,
		// functions.length(df.col("name")).minus(1)));
		// so we should use substr function  index 1 based
		df.withColumn("name", functions.substr(df.col("name"), functions.lit(2), functions.length(df.col("name"))))
				.show();
		// index 1 based 
		df.withColumn("name", functions.expr("substring(name,2,length(name))")).show();
	}
}
