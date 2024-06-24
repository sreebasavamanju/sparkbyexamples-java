package com.learning.sparkbyexamples.problems.stackoverflow;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/*
 * Explode the struct type  column
 * https://stackoverflow.com/questions/78628216/pyspark-explode-a-dataframe-col-which-contains-json
 */
public class ExplodeJsonColumn {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Spark Explode Json value column").master("local[*]")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		List<Row> rowList = new ArrayList<>();
		rowList.add(RowFactory.create("Test", RowFactory.create(RowFactory.create("16", "sam", "2.00"))));

		StructType schema = new StructType().add("name", DataTypes.StringType).add("user_contacts_attributes",
				new StructType().add("user", new StructType().add("id", DataTypes.StringType)
						.add("level", DataTypes.StringType).add("username", DataTypes.StringType)));
		schema.printTreeString();

		Dataset<Row> df = spark.createDataFrame(rowList, schema);
		df.show(false);

		// below will throw error, explode is applicable for array or map type column not for struct type
		//df.withColumn("exploded", functions.explode(df.col("user_contacts_attributes.user"))).show();
		 
		df.select("name","user_contacts_attributes.user.*").show();

	}
}
