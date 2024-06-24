package com.learning.sparkbyexamples.problems.stackoverflow;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * https://stackoverflow.com/questions/78648876/nested-condition-on-simple-data
 */
public class NestedCondition {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Nested Condition").master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		List<Row> rowList = new ArrayList<>();
		rowList.add(RowFactory.create(true, "CA", null));
		rowList.add(RowFactory.create(true, "US", null));
		rowList.add(RowFactory.create(false, "CA", null));

		StructType schema = new StructType().add("is_flag", DataTypes.BooleanType).add("country", DataTypes.StringType)
				.add("rule", DataTypes.BooleanType, true);

		Dataset<Row> df = spark.createDataFrame(rowList, schema);
		df.show();

		Column conditions = functions.not(functions.col("is_flag"))
				.or(functions.col("is_flag").equalTo(true).and(functions.trim(functions.col("country")).notEqual("CA")))
						.and(functions.nvl(functions.col("rule"), functions.lit(false)).notEqual(true));
		
		df.filter(conditions).show();

	}
}
