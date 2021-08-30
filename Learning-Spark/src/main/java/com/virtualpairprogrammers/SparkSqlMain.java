package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class SparkSqlMain {

	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("testingSQL").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();
		Dataset<Row> dataset =  spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		dataset.show();
		Row firstRow = dataset.first();
		String subject = firstRow.getString(2);
		System.out.println("Subject is " + subject);
		
		int year = Integer.parseInt(firstRow.getAs("year"));
		System.out.println("year is " + year);
		//Dataset<Row> modernArtResults = dataset.filter("subject == 'Modern Art' and year >= 2008");
		//Dataset<Row> modernArtResults = dataset.filter(row -> row.getAs("subject").equals("Modern Art") && Integer.parseInt(row.getAs("year")) == 2005);
//		Column subjectColumn = dataset.col("subject");
//		Column yearColumn = dataset.col("year");
//		Dataset<Row> modernArtResults = dataset.filter(subjectColumn.equalTo("Modern Art").and(yearColumn.equalTo(2006)));
		Dataset<Row> modernArtResults = dataset.filter(col("subject").equalTo("Modern Art").and(col("year").equalTo(2009)));
		modernArtResults.show();
		dataset.createOrReplaceTempView("my_student_view");
//		Dataset<Row> frenchResults = spark.sql("select avg(score) from my_student_view where subject='French'");
		Dataset<Row> frenchResults = spark.sql("select distinct year from my_student_view where subject='French' order by year desc");
		frenchResults.show();
		spark.close();
	}

}
