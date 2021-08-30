package com.virtualpairprogrammers;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class KeywordRankingTest {

	public static void main(String args[]) {
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
		JavaRDD<String> lettersOnlyRdd = initialRdd.map( sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase().trim() );
		JavaRDD<String> nonBoringWords = lettersOnlyRdd.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
				.filter(word -> Util.isNotBoring(word));
		JavaPairRDD<String, Long> mapResult = nonBoringWords.mapToPair(word -> {
			return new Tuple2<>(word, 1L);
		});
		JavaPairRDD<String, Long> reduceResult = mapResult.reduceByKey((value1, value2) -> value1 + value2);
		reduceResult.sortByKey(false).take(10).forEach(System.out::println);
		sc.close();
	}
}
