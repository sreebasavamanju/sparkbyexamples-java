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
 * https://stackoverflow.com/questions/78602000/compare-two-dataframe-using-pyspark-in-azure-data-bricks
 */
public class CompareTwoDf {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local[*]")
				.appName("Spark compare two df and update the new column if value is present").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		List<Row> dfOne = new ArrayList<>();
		dfOne.add(RowFactory.create(01, true, 2304));
		dfOne.add(RowFactory.create(02, true, 5654));
		dfOne.add(RowFactory.create(03, false, 6785));
		dfOne.add(RowFactory.create(04, true, null));
		dfOne.add(RowFactory.create(05, false, 6785));

		StructType schema = new StructType().add("id", DataTypes.IntegerType).add("inUse", DataTypes.BooleanType)
				.add("otherId", DataTypes.IntegerType, true);

		Dataset<Row> df = spark.createDataFrame(dfOne, schema);
		df.show();

		List<Row> dfTwoList = new ArrayList<>();
		dfTwoList.add(RowFactory.create(01, 2304));
		dfTwoList.add(RowFactory.create(02, 5654));
		dfTwoList.add(RowFactory.create(04, 37584));
		dfTwoList.add(RowFactory.create(05, 6785));

		StructType dfTwoSchmea = new StructType().add("id", DataTypes.IntegerType).add("otherId", DataTypes.IntegerType,
				true);

		Dataset<Row> dfTwo = spark.createDataFrame(dfTwoList, dfTwoSchmea);
		dfTwo.show();

		Dataset<Row> joinDF = df.join(dfTwo, df.col("id").equalTo(dfTwo.col("id")), "right");
		Dataset<Row> withColumn = joinDF.withColumn("Presentin_col",
				functions.when(df.col("otherId").equalTo(dfTwo.col("otherId")), functions.lit(true))
						.otherwise(functions.lit(false)));
		withColumn.select(dfTwo.col("id"), dfTwo.col("otherId"), withColumn.col("Presentin_col")).show();
	}

}
