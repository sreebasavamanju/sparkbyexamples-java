package com.learning.sparkbyexamples.problems.stackoverflow;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * https://stackoverflow.com/questions/78614880/pyspark-3-3-extract-all-regex-and-replace-with-another-dataframe
 * 
 * https://stackoverflow.com/questions/35348058/how-do-i-call-a-udf-on-a-spark-dataframe-using-java - UDF
 */
public class SparkExtractReplaceUDF {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Spark extract Replace from another DF").master("local[*]")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		List<Row> rowList = new ArrayList<>();
		rowList.add(RowFactory.create("MONOCYTES 1511|A5905.5"));

		StructType schema = new StructType().add("value", DataTypes.StringType);

		Dataset<Row> df = spark.createDataFrame(rowList, schema);
		df.show(false);

		List<Row> codeRowList = new ArrayList<>();
		codeRowList.add(RowFactory.create("1511", "monocytes1"));
		codeRowList.add(RowFactory.create("A5905.5", "monocytes2"));
		StructType codeSchema = new StructType().add("code", DataTypes.StringType).add("value", DataTypes.StringType);

		Dataset<Row> codeDF = spark.createDataFrame(codeRowList, codeSchema);
		codeDF.show();

		UserDefinedFunction findUdf = functions.udf((String s) -> s.split("\\w?\\d+(?:\\.\\d+)?"),
				DataTypes.createArrayType(DataTypes.StringType));
		df.withColumn("Split_values", findUdf.apply(df.col("value"))).show();
	}

}
