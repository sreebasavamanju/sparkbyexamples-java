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
 * https://stackoverflow.com/questions/78651171/pyspark-builtin-functions-to-remove-udf
 */
public class SparkRegexReplace {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Spatrk Regex Repalce Example ").master("local[*]")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		List<Row> rowList = new ArrayList<>();
		rowList.add(RowFactory.create(
				"\"{\"\"ab\"\": 0.7220268151565864, \"\"cd\"\": 0.2681795338834256, \"\"ef\"\": 1.0, \"\"gh\"\": 1.0, \"\"ij\"\": 0.9266362339932378, \"\"kl\"\": 0.7002315808130385}\"",
				"\"{\"\"mn\"\": 0.7220268151565864, \"\"op\"\": 0.2681795338834256, \"\"qr\"\": 1.0, \"\"st\"\": 1.0, \"\"uv\"\": 0.9266362339932378, \"\"wx\"\": 0.7002315808130385}\""));

		StructType schema = new StructType().add("col1", DataTypes.StringType).add("col2", DataTypes.StringType);

		Dataset<Row> df = spark.createDataFrame(rowList, schema);
		df.show(false);

		for (String colName : df.columns()) {
			df = df.withColumn(colName, functions
					.regexp_replace(functions.regexp_replace(functions.col(colName), "\"\"", "\""), "^\"|\"$", ""));

		}
		df.show(false);
	}
}
