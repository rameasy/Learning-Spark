package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.date_format;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
public class Grouping {
	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();

		List<Row> inMemory = new ArrayList<Row>();
		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
		inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
		inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
		inMemory.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));

		StructField[] structFields = new StructField[] {
				new StructField("level", DataTypes.StringType, false, Metadata.empty()),
				new StructField("datetime", DataTypes.StringType, false, Metadata.empty()) };

		StructType structType = new StructType(structFields);

		//Dataset<Row> df = spark.createDataFrame(inMemory, structType);
		Dataset<Row> df = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

		//df.createOrReplaceTempView("logging_table");
		//spark.sql("select level, count(datetime) from logging_table group by level").show();

		 //spark.sql("select level, collect_list(date_format(datetime,'MMMM')) as years_group from logging_table group by level").show();

		// now for the pivot
		df.select(date_format(col("datetime"), "y").alias("year"), col("level")).groupBy("level").pivot("year").agg(collect_list("year")).na().fill(0).show();

		 //df.groupBy("level").pivot("datetime").count().na().fill(0).show();

		/*
		 * Dataset<Row> results = spark.
		 * sql("select level, date_format(datetime, 'MMMM')as month, cast(first(date_format(datetime, 'M')) as int) as Num_month, count(1)  from logging_table group by level, month order by Num_month, level"
		 * ); results = results.drop("Num_month"); results.show();
		 */
	}
}
