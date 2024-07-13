package com.learning.sparkbyexamples.problems;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkMultiDelimiterColumns {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Spark Read text file into DF").master("local[*]")
				.getOrCreate();

		Dataset<String> textDF = spark.read().textFile(args[0]);

		StructType schema = new StructType().add("id", DataTypes.StringType).add("fname", DataTypes.StringType)
				.add("lname", DataTypes.StringType).add("salary", DataTypes.StringType);

		Dataset<Row> df = textDF.map((MapFunction<String, Row>) row -> {
			String[] split = row.split(",");
			if (split.length == 4) {
				return RowFactory.create(split[0], split[1], split[2], split[3]);
			}
			String[] split2 = split[2].split("\\|");
			return RowFactory.create(split[0], split[1], split2[0], split2[1]);
		}, Encoders.row(schema));

		df.show();

	}
}
