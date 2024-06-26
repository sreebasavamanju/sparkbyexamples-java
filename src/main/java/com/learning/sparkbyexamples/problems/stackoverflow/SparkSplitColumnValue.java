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
 * https://stackoverflow.com/questions/78588236/split-value-column-name-and-rest-of-the-value
 * https://stackoverflow.com/questions/39746752/how-to-get-name-of-dataframe-column-in-pyspark
 */
public class SparkSplitColumnValue {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Spark Split column value").master("local[*]")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		List<Row> rowList = new ArrayList<>();

		rowList.add(RowFactory.create("genre_list_comedy_val", "test_list_abc"));
		rowList.add(RowFactory.create("genre_list_drama_val_us", "test_list_def"));
		rowList.add(RowFactory.create("genre_list_action_val", "test_list_gh"));

		StructType schema = new StructType().add("genre_list", DataTypes.StringType).add("test_list",
				DataTypes.StringType);
		Dataset<Row> df = spark.createDataFrame(rowList, schema);
		df.show(false);

		for (String name : df.schema().names()) {
			df = df.withColumn(name + "_collumn_name", functions.lit(name))
					.withColumn(name + "_value", functions.split(df.col(name), name + "_").getItem(1))
					.drop(functions.col(name));
		}

		df.show(false);
	}
}
