package edu.upenn.cis455.searchengine;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.Tuple3;

@SuppressWarnings("serial")
public class SparkWrapper implements Serializable{

	public static Logger log = Logger.getLogger(SparkWrapper.class);
	
	private SparkSession spark;
	
	static final Pattern LINE = Pattern.compile("\t\n");
	static final Pattern DILIMETER = Pattern.compile(" ");
	
	public SparkWrapper() {
	}
	
	public SparkSession init() {
		spark = SparkSession
				.builder()
				.master("local[4]")
				.appName("SearchCount")
				.getOrCreate();
		return spark;
	}
	
	public List<Tuple2<String,Tuple3<List<String>, Double, List<String>>>> startSearchCount(String input) {
	
		JavaRDD<String> lines = spark.read().textFile(input).javaRDD();

		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String s) {
				return Arrays.asList(LINE.split(s)).iterator();
			}
		});

		JavaPairRDD<String, Tuple3<List<String>, Double, List<String>>> map = words.mapToPair(
				new PairFunction<String, String, Tuple3<List<String>, Double, List<String>>>() {
					@Override
					public Tuple2<String, Tuple3<List<String>, Double,List<String>>> call(String s) {
//						List<> l = Arrays.asList(LINE.split(s));
						String[] values = s.split(" ");
						double weights = Double.parseDouble(values[2]);
						List<String> words = new ArrayList<>();
						List<String> titles = new ArrayList<>();
						words.add(values[0]);
						titles.add(values[3]);
						return new Tuple2<>(values[1],new Tuple3<>(words, weights, titles));
					}
				});

		
		//doc: ([word]+, tf*idf*pr, [boolean]+)
		//url: ([word1, word2], weights, [T,F])
		JavaPairRDD<String, Tuple3<List<String>, Double, List<String>>> counts = map.reduceByKey(
				new Function2<Tuple3<List<String>, Double, List<String>>, Tuple3<List<String>, Double, List<String>>, Tuple3<List<String>, Double, List<String>>>() {
					@Override
					public Tuple3<List<String>, Double, List<String>> call(Tuple3<List<String>, Double, List<String>> t1, Tuple3<List<String>, Double, List<String>> t2) {
						List<String> words = t1._1();
						words.addAll(t2._1());
						List<String> weights = t1._3();
						weights.addAll(t2._3());
						
						return new Tuple3<>(words, t1._2()+t2._2(), weights);
					}
				});

		List<Tuple2<String,Tuple3<List<String>, Double, List<String>>>> output = new ArrayList<>(counts.collect());
		Collections.sort(output, (t1, t2) -> t1._2._1().size() == t2._2._1().size() ?t2._2._2().compareTo(t1._2._2()) : t2._2._1().size() - t1._2._1().size());
		return output;
	}
		
	
	public void stopSpark() {
		spark.stop();
	}
}
