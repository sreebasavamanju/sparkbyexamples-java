package com.learning.sparkbyexamples.sql.functions.window;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.cume_dist;
import static org.apache.spark.sql.functions.dense_rank;
import static org.apache.spark.sql.functions.lag;
import static org.apache.spark.sql.functions.lead;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.ntile;
import static org.apache.spark.sql.functions.percent_rank;
import static org.apache.spark.sql.functions.rank;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.sum;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * https://sparkbyexamples.com/spark/spark-sql-window-functions/
 * 
 * Spark Window functions operate on a group of rows (like frame, partition) and
 * return a single value for every input row. Spark SQL supports three kinds of
 * window functions:
 * 
 * <li>ranking functions</li>
 * <li>analytic functions</li>
 * <li>aggregate functions</li>
 */
public class SparkWindowFunctions {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local[*]").appName("Spark Window Functions").getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");

		// create a Dataframe
		List<Row> rowsList = new ArrayList<>();

		rowsList.add(RowFactory.create("James", "Sales", 3000));
		rowsList.add(RowFactory.create("Michael", "Sales", 4600));
		rowsList.add(RowFactory.create("Robert", "Sales", 4100));
		rowsList.add(RowFactory.create("Maria", "Finance", 3000));
		rowsList.add(RowFactory.create("James", "Sales", 3000));
		rowsList.add(RowFactory.create("Scott", "Finance", 3300));
		rowsList.add(RowFactory.create("Jen", "Finance", 3900));
		rowsList.add(RowFactory.create("Jeff", "Marketing", 3000));
		rowsList.add(RowFactory.create("Kumar", "Marketing", 2000));
		rowsList.add(RowFactory.create("Saif", "Sales", 4100));

		StructType schema = new StructType().add("employee_name", DataTypes.StringType)
				.add("department", DataTypes.StringType).add("salary", DataTypes.IntegerType);

		Dataset<Row> df = spark.createDataFrame(rowsList, schema);

		df.printSchema();
		df.show(false);

		// Spark Window Ranking functions
		/**
		 * 2.1 row_number Window Function row_number() window function is used to give
		 * the sequential row number starting from 1 to the result of each window
		 * partition.
		 *
		 */

		WindowSpec windowSpec = Window.partitionBy(col("department")).orderBy(col("salary"));

		df.withColumn("row_number", row_number().over(windowSpec)).show(false);

		/**
		 * rank Window Function rank() window function is used to provide a rank to the
		 * result within a window partition. This function leaves gaps in rank when
		 * there are ties.
		 */

		df.withColumn("rank", rank().over(windowSpec)).show();

		/**
		 * dense_rank Window Function dense_rank() window function is used to get the
		 * result with rank of rows within a window partition without any gaps. This is
		 * similar to rank() function difference being rank function leaves gaps in rank
		 * when there are ties.
		 * 
		 */

		df.withColumn("dense_rank", dense_rank().over(windowSpec)).show();

		/**
		 * percent_rank Window Function
		 */

		df.withColumn("percent_rank", percent_rank().over(windowSpec)).show();

		/**
		 * ntile Window Function ntile() window function returns the relative rank of
		 * result rows within a window partition. In below example we have used 2 as an
		 * argument to ntile hence it returns ranking between 2 values (1 and 2)
		 */

		df.withColumn("ntile", ntile(2).over(windowSpec)).show();

		// Spark Window Analytic functions

		/**
		 * cume_dist Window Function cume_dist() window function is used to get the
		 * cumulative distribution of values within a window partition.
		 * 
		 * This is the same as the DENSE_RANK function in SQL.
		 */

		df.withColumn("cume_dist", cume_dist().over(windowSpec)).show();

		/**
		 * lag Window Function
		 */
		df.withColumn("lag", lag("salary", 2).over(windowSpec)).show();

		/**
		 * lead Window Function
		 */

		df.withColumn("lead", lead("salary", 2).over(windowSpec)).show();

		/**
		 * Spark Window Aggregate Functions In this section, I will explain how to
		 * calculate sum, min, max for each department using Spark SQL Aggregate window
		 * functions and WindowSpec. When working with Aggregate functions, we don’t
		 * need to use order by clause.
		 */

		windowSpec = Window.partitionBy("department").orderBy("salary");
		WindowSpec windowSpecAgg = Window.partitionBy("department");
		df.withColumn("row", row_number().over(windowSpec)).withColumn("avg", avg(col("salary")).over(windowSpecAgg))
				.withColumn("sum", sum(col("salary")).over(windowSpecAgg))
				.withColumn("min", min(col("salary")).over(windowSpecAgg))
				.withColumn("max", max(col("salary")).over(windowSpecAgg)).where(col("row").equalTo(1))
				.select("row","department", "avg", "sum", "min", "max").show();
	}

}
