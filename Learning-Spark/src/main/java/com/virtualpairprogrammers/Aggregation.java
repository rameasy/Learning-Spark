package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;

import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class Aggregation {

	public static void main(String[] args)
	{
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("spark://192.168.0.16:7077")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")
				                                   .getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		
		dataset = dataset.groupBy("subject").agg(max(col("score").cast(DataTypes.IntegerType)).alias("max score") ,
				                                 min(col("score").cast(DataTypes.IntegerType)).alias("min score"));
		
		dataset.show();
		/*
		 * dataset = dataset.groupBy("subject").agg(max(col("score")).alias("max score")
		 * , min(col("score")).alias("min score")); dataset.show();
		 */
		Scanner scan = new Scanner(System.in);
		scan.nextLine();
		spark.close();
	}

}
