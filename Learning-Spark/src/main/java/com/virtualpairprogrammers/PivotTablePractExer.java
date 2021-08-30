package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class PivotTablePractExer {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();

		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

		// dataset =
		// dataset.groupBy("subject","year").avg(max(col("score").cast(DataTypes.IntegerType)).alias("max
		// score"));

		dataset = dataset.groupBy("subject").pivot("year").agg(round(avg(col("score").cast(DataTypes.IntegerType)), 2).alias("avg"),
				round(stddev(col("score")),2).alias("stddev"));

		dataset.show(100);

		spark.close();
	}

}
