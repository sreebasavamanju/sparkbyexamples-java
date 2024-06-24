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
 * https://stackoverflow.com/questions/78637317/is-there-a-way-to-store-a-dictionary-as-a-column-value-in-pyspark
 * 
 */
public class SparkMapAsColumnValue {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Store Map as a column value").master("local[*]")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		List<Row> tableOne = new ArrayList<>();
		tableOne.add(RowFactory.create("1", 34, 45));
		tableOne.add(RowFactory.create("2", 78, 89));

		List<Row> tableTwo = new ArrayList<>();
		tableTwo.add(RowFactory.create("1", 43, 54));
		tableTwo.add(RowFactory.create("2", 11, 12));

		StructType schema = new StructType().add("id", DataTypes.StringType).add("col1", DataTypes.IntegerType)
				.add("col2", DataTypes.IntegerType);

		Dataset<Row> df1 = spark.createDataFrame(tableOne, schema);
		df1.show();
		Dataset<Row> df2 = spark.createDataFrame(tableTwo, schema);
		df2.show();

		Dataset<Row> withColumn = df1.withColumn("table1_cols", functions.map(functions.lit("col1"),
				functions.col("col1"), functions.lit("col2"), functions.col("col2")));
		withColumn.show(false);

		Dataset<Row> withColumn1 = df2.withColumn("table2_cols", functions.map(functions.lit("col1"),
				functions.col("col1"), functions.lit("col2"), functions.col("col2")));
		withColumn1.show(false);

		withColumn.join(withColumn1, withColumn.col("id").equalTo(withColumn1.col("id")), "inner")
				.select(withColumn1.col("id"), functions.col("table1_cols"), functions.col("table2_cols")).show(false);

	}
}
