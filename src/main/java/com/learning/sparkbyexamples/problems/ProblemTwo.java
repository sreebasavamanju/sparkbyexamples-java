package com.learning.sparkbyexamples.problems;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class ProblemTwo {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Problem and solutions").master("local[*]").getOrCreate();

		// Question 2:
		/*
		 * ------------------------Problem statement --------------------------------
		 * There are two dataframes: “BlogPosts” and “Comments”. The “BlogPosts” table
		 * has columns: PostID, Title, PostDate, AuthorID. The “Comments” table has
		 * columns: CommentID, PostID, CommentDate, Text. Write a SQL query /Spark code
		 * to fetch the blog posts that have not received any comments within a week of
		 * their posting.
		 */

		List<Row> blogPostList = new ArrayList<>();
		blogPostList.add(RowFactory.create(1, "First Blog Post", "2023-01-01 00:00:00", 10));
		blogPostList.add(RowFactory.create(2, "Second Blog Post", "2023-01-15 00:00:00", 102));
		blogPostList.add(RowFactory.create(3, "Third Blog Post", "2023-02-01 00:00:00", 101));
		blogPostList.add(RowFactory.create(4, "Fourth Blog Post", "2023-01-01 00:00:00", 101));

		StructType blogPostSchema = new StructType().add("PostID", DataTypes.IntegerType)
				.add("Title", DataTypes.StringType).add("PostDate", DataTypes.StringType)
				.add("AuthorId", DataTypes.IntegerType);

		List<Row> commentstList = new ArrayList<>();
		commentstList.add(RowFactory.create(1, 1, "2023-01-02 00:00:00", "Great Post!"));
		commentstList.add(RowFactory.create(2, 1, "2023-01-03 00:00:00", "Thanks for sharing"));
		commentstList.add(RowFactory.create(3, 2, "2023-01-16 00:00:00", "Very informative"));
		commentstList.add(RowFactory.create(4, 3, "2023-02-20 00:00:00", "Interesting read"));
		commentstList.add(RowFactory.create(5, 1, "2023-01-30 00:00:00", "Good post!"));

		StructType commentsSchema = new StructType().add("CommentID", DataTypes.IntegerType)
				.add("PostID", DataTypes.IntegerType).add("CommentDate", DataTypes.StringType)
				.add("Text", DataTypes.StringType);

		Dataset<Row> blogPostDF = spark.createDataFrame(blogPostList, blogPostSchema);
		blogPostDF = blogPostDF.withColumn("PostDate", functions.to_timestamp(blogPostDF.col("PostDate")));
		blogPostDF.printSchema();
		blogPostDF.show();

		Dataset<Row> commentsDF = spark.createDataFrame(commentstList, commentsSchema);
		commentsDF = commentsDF.withColumn("CommentDate", functions.to_timestamp(commentsDF.col("CommentDate")));
		commentsDF.printSchema();
		commentsDF.show();

		blogPostDF
				.join(commentsDF,
						blogPostDF.col("PostID").equalTo(commentsDF.col("PostID"))
								.and(functions.date_diff(commentsDF.col("CommentDate"), blogPostDF.col("PostDate"))
										.leq(functions.lit(7))),
						"left")
				.where(commentsDF.col("commentId").isNull()).show();
		//
		blogPostDF.createOrReplaceTempView("blogs");
		commentsDF.createOrReplaceTempView("comments");
		spark.sql(
				"select * from blogs LEFT JOIN comments ON blogs.PostID == comments.PostID AND date_diff(comments.CommentDate,blogs.PostDate)<=7 WHERE comments.CommentID IS NULL ")
				.show();
	}
}
