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

import scala.collection.immutable.ArraySeq;
import scala.collection.JavaConverters;

/**
 * https://stackoverflow.com/questions/78599998/spark-dataframe-to-check-if-all-the-elements-are-matched-to-given-value-of-parti
 * 
 * https://stackoverflow.com/questions/48440869/apache-spark-row-of-multiple-string-fields-to-single-row-with-string-array-conve
 * https://stackoverflow.com/questions/59026439/how-to-create-a-spark-udf-in-java-which-accepts-array-of-strings
 * https://stackoverflow.com/questions/42791931/java-spark-udf-that-returns-array-of-integers-gives-me-classexception
 */
public class SparkArrayAllElementsCheck {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Spark Array Check alle elemnts matched or not")
				.master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		List<Row> rowList = new ArrayList<>();
		rowList.add(RowFactory.create(1000, new boolean[] { true, true, true }));
		rowList.add(RowFactory.create(1001, new boolean[] { true, false, true }));
		rowList.add(RowFactory.create(1002, new boolean[] { true, true, true }));

		StructType schema = new StructType().add("emp_id", DataTypes.IntegerType).add("result",
				DataTypes.createArrayType(DataTypes.BooleanType));

		Dataset<Row> df = spark.createDataFrame(rowList, schema);
		df.show();

		UserDefinedFunction isAllTrue = functions.udf((ArraySeq<Boolean> arr) -> {
			List<Boolean> booleanList = JavaConverters.asJava(arr);
			for (boolean bool : booleanList) {
				if (!bool) {
					return false;
				}
			}
			return true;
		}, DataTypes.BooleanType);

		df.withColumn("output", isAllTrue.apply(df.col("result"))).show();

		// for this case it will work because the values are either treu or false
		df.withColumn("output", functions.not(functions.array_contains(df.col("result"), false))).show();
	}
}
