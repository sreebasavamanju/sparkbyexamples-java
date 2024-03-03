package com.learning.sparkbyexamples.sql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * https://sparkbyexamples.com/spark/spark-map-vs-mappartitions-transformation/
 * 
 * Spark map() and mapPartitions() transformations apply the function on each
 * element/record/row of the DataFrame/Dataset and returns the new
 * DataFrame/Dataset
 * 
 * map() – Spark map() transformation applies a function to each row in a
 * DataFrame/Dataset and returns the new transformed Dataset.
 * 
 * mapPartitions() - This is exactly the same as map(); the difference being,
 * Spark mapPartitions() provides a facility to do heavy initializations (for
 * example Database connection) once for each partition instead of doing it on
 * every DataFrame row. This helps the performance of the job when you dealing
 * with heavy-weighted initialization on larger datasets.
 */
public class MapTransformation implements Serializable {

	private static final long serialVersionUID = 1L;

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local[1]").appName("Spark - Map() and mappartitions()")
				.getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");
		// Create DataFrame

		List<Row> rowsList = new ArrayList<>();
		rowsList.add(RowFactory.create("James", "", "Smith", "36636", "NewYork", 3100));
		rowsList.add(RowFactory.create("Michael", "Rose", "", "40288", "California", 4300));
		rowsList.add(RowFactory.create("Robert", "", "Williams", "42114", "Florida", 1400));
		rowsList.add(RowFactory.create("Maria", "Anne", "Jones", "39192", "Florida", 5500));
		rowsList.add(RowFactory.create("Jen", "Mary", "Brown", "34561", "NewYork", 3000));

		StructType structureSchema = new StructType().add("firstname", DataTypes.StringType)
				.add("middlename", DataTypes.StringType).add("lastname", DataTypes.StringType)
				.add("id", DataTypes.StringType).add("location", DataTypes.StringType)
				.add("salary", DataTypes.IntegerType);

		Dataset<Row> df = spark.createDataFrame(rowsList, structureSchema);
		df.printSchema();
		df.show();

		StructType newMapSchema = new StructType().add("fullName", DataTypes.StringType).add("id", DataTypes.StringType)
				.add("salary", DataTypes.IntegerType);
		Dataset<Row> mapDF = df.map((MapFunction<Row, Row>) row -> {
			String fullName = new Utils().combineStrings(row.getString(0), row.getString(1), row.getString(2));
			return RowFactory.create(fullName, row.getString(3), row.getInt(5));
		}, Encoders.row(newMapSchema));

		mapDF.printSchema();
		mapDF.show(false);

		// Spark mapPartitions() transformation

		/**
		 * Spark mapPartitions() provides a facility to do heavy initializations (for
		 * example Database connection) once for each partition instead of doing it on
		 * every DataFrame row. This helps the performance of the job when you dealing
		 * with heavy-weighted initialization on larger datasets.
		 */

		Dataset<Row> mapPartitionsDF = df.mapPartitions((MapPartitionsFunction<Row, Row>) itr -> {

			List<Row> ls = new LinkedList<>();
			Utils utils = new Utils();
			while (itr.hasNext()) {
				Row row = itr.next();
				String fullName = utils.combineStrings(row.getString(0), row.getString(1), row.getString(2));
				ls.add(RowFactory.create(fullName, row.getString(3), row.getInt(5)));
			}
			return ls.iterator();

		}, Encoders.row(newMapSchema));

		// BELOW code throws task not serializable , as of now parked, didn't abe to
		// solve that, might be due to scala function and iterator signature is used
//		Dataset<Row> mapPartitionsDF = df.mapPartitions((Function1<Iterator<Row>, Iterator<Row>>) itr -> {
//			Utils utils = new Utils();
//			Iterator<Row> mapRes = itr.map(row -> {
//				String fullName = utils.combineStrings(row.getString(0), row.getString(1),
//						row.getString(2));
//				return RowFactory.create(fullName, row.getString(3), row.getInt(5));
//			});
//			return mapRes;
//		}, Encoders.row(newMapSchema));

		mapPartitionsDF.printSchema();

		mapPartitionsDF.show();

	}
}
