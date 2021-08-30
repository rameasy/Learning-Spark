package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * This class is used in the chapter late in the course where we analyse viewing
 * figures. You can ignore until then.
 */
public class ViewingFigures {
	private static Logger log = Logger.getLogger(ViewingFigures.class);

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Use true to use hardcoded data identical to that in the PDF guide.
		boolean testMode = false;

		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

		JavaPairRDD<Integer, Integer> chapterCount = chapterData
				.mapToPair(row -> new Tuple2<Integer, Integer>(row._2, 1))
				.reduceByKey((value1, value2) -> value1 + value2);

		// Step 1 - Removing duplicates from viewData
		JavaPairRDD<Integer, Integer> distinctViewData = viewData.distinct();
		// distinctViewData.collect().forEach(System.out::println);
		// Step 2 - Joining viewData and chapterData
		JavaPairRDD<Integer, Integer> flippedViewData = distinctViewData
				.mapToPair(row -> new Tuple2<Integer, Integer>(row._2, row._1));

		JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedData = flippedViewData.join(chapterData);

		// Step 3 - Map the pair
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> mappedData = joinedData
				.mapToPair(row -> new Tuple2<Tuple2<Integer, Integer>, Integer>(row._2, 1));

		// Step 4 - reduce the pair
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> reducedData = mappedData
				.reduceByKey((value1, value2) -> value1 + value2);

		// Step 5 - Dropping the user id
		JavaPairRDD<Integer, Integer> userData = reducedData.mapToPair(record -> new Tuple2<Integer, Integer>(record._1()._2, record._2));

		// Step 6 - join with Chapter count to get the number of chapters
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> chapterViewData = userData.join(chapterCount);

		// Step 7 - Convert to percentage
		JavaPairRDD<Integer, Double> percentageData = chapterViewData
				.mapToPair(record -> new Tuple2<Integer, Double>(record._1, (double) record._2._1 / record._2._2));

		// Step 8 - Convert to Scores
		JavaPairRDD<Integer, Integer> scoreData = percentageData
				.mapToPair(record -> {
					int key, value;
					key  = record._1;
					double score = record._2;
					if(score >= 0.9) {
						value = 10;
					} else if(score >= .5 && score < 0.9) {
						value = 4;
					} else if(score >= .25 && score < 0.5) {
						value = 2;
					} else {
						value = 0;
					}
					return new Tuple2<Integer, Integer>(key, value);
				});
		// Step 9 - Add score data
		JavaPairRDD<Integer, Integer> resultData = scoreData
				.reduceByKey((value1, value2) -> value1 + value2);
		
		//excercise 3:: sort the data by value
		JavaPairRDD<Integer, Integer> flippedResultData = resultData.mapToPair(record -> new Tuple2<Integer, Integer>(record._2, record._1));
		flippedResultData.sortByKey(false).take(200).forEach(System.out::println);
		sc.close();
	}

	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {

		if (testMode) {
			// (chapterId, title)
			List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
		}
		return sc.textFile("src/main/resources/viewing figures/titles.csv").mapToPair(commaSeparatedLine -> {
			String[] cols = commaSeparatedLine.split(",");
			return new Tuple2<Integer, String>(new Integer(cols[0]), cols[1]);
		});
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {

		if (testMode) {
			// (chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96, 1));
			rawChapterData.add(new Tuple2<>(97, 1));
			rawChapterData.add(new Tuple2<>(98, 1));
			rawChapterData.add(new Tuple2<>(99, 2));
			rawChapterData.add(new Tuple2<>(100, 3));
			rawChapterData.add(new Tuple2<>(101, 3));
			rawChapterData.add(new Tuple2<>(102, 3));
			rawChapterData.add(new Tuple2<>(103, 3));
			rawChapterData.add(new Tuple2<>(104, 3));
			rawChapterData.add(new Tuple2<>(105, 3));
			rawChapterData.add(new Tuple2<>(106, 3));
			rawChapterData.add(new Tuple2<>(107, 3));
			rawChapterData.add(new Tuple2<>(108, 3));
			rawChapterData.add(new Tuple2<>(109, 3));
			return sc.parallelizePairs(rawChapterData);
		}

		return sc.textFile("src/main/resources/viewing figures/chapters.csv").mapToPair(commaSeparatedLine -> {
			String[] cols = commaSeparatedLine.split(",");
			return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
		});
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {

		if (testMode) {
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(14, 99));
			rawViewData.add(new Tuple2<>(13, 100));
			return sc.parallelizePairs(rawViewData);
		}

		return sc.textFile("src/main/resources/viewing figures/views-*.csv").mapToPair(commaSeparatedLine -> {
			String[] columns = commaSeparatedLine.split(",");
			return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
		});
	}
}
