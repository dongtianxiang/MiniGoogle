package edu.upenn.cis455.searchengine;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.Tuple3;

@SuppressWarnings("serial")
public class SparkWrapper implements Serializable{

	public static Logger log = Logger.getLogger(SparkWrapper.class);
	
	private SparkSession spark;
	
	static Semaphore mutex = new Semaphore(1);
	
	static final Pattern LINE = Pattern.compile("\t\n");
//	static final Pattern DILIMETER = Pattern.compile(" ");
	static final Pattern DILIMETER = Pattern.compile("#");
	static Set<String> urlSet = new HashSet<>();
    static Set<String> ignore = new HashSet<>();
    static {

        ignore.add("https:");
        ignore.add("http:");
        ignore.add("com");
        ignore.add("edu");
        ignore.add("it");
        ignore.add("uk");
        ignore.add("net");
        ignore.add("gov");
        ignore.add("org");
        ignore.add("www");
    }
	
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
	
	public List<Tuple3<String, List<String>, Double>> startSearchCount(String input) {
		urlSet.clear();
		JavaRDD<String> lines = spark.read().textFile(input).javaRDD();

		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String s) {
				return Arrays.asList(LINE.split(s)).iterator();
			}
		});
		
		JavaPairRDD<String, Tuple2<List<String>, Double>> mapping = words
				.mapToPair(new PairFunction<String, String, Tuple2<List<String>, Double>>() {
					@Override
					public Tuple2<String, Tuple2<List<String>, Double>> call(String s) throws MalformedURLException {

						String[] values = s.split("#");
						double weight = Double.parseDouble(values[2]);
						
						if (values[3].equals("T")) {
							weight *= 1.5;
						}
						
						List<String> words = new ArrayList<>();
						String url = values[1];

						URL u = new URL(url);
						String path = u.getPath();
						if (path.contains(values[0])) {
							weight *= 1.5;
						}
						if (url.contains("wikipedia") && url.toLowerCase().contains(values[0])) {
							String[] check = values[0].split(" ");
							if (check.length == 3) {
								weight *= 30;
							} else if (check.length == 2) {
								weight *= 20;
							} else {
								weight *= 10;
							}
						}

						words.add(values[0]);
						String normUrl = values[1];
						if(normUrl.startsWith("https")) {
							normUrl = normUrl.substring(5);
						} else {
							normUrl = normUrl.substring(4);
						}
						if(normUrl.endsWith("/")) {
							normUrl = normUrl.substring(0, normUrl.length()-1);
						}
						if(!urlSet.contains(normUrl)) {
							if(urlSet.contains("http"+normUrl)) {
								return new Tuple2<>("http"+normUrl, new Tuple2<>(words, weight));
							} else if(urlSet.contains("https"+normUrl)) {
								return new Tuple2<>("https"+normUrl, new Tuple2<>(words, weight));
							} else if (urlSet.contains("https"+normUrl+"/")) {
								return new Tuple2<>("https"+normUrl+"/", new Tuple2<>(words, weight));
							} else if (urlSet.contains("http"+normUrl+"/")){
								return new Tuple2<>("http"+normUrl+"/", new Tuple2<>(words, weight));
							}
						}
						
						urlSet.add(url);
						return new Tuple2<>(url, new Tuple2<>(words, weight));
					}
				})

				.reduceByKey(
						new Function2<Tuple2<List<String>, Double>, Tuple2<List<String>, Double>, Tuple2<List<String>, Double>>() {

							@Override
							public Tuple2<List<String>, Double> call(Tuple2<List<String>, Double> t1,
									Tuple2<List<String>, Double> t2) throws Exception {
								
													
								List<String> temp = new ArrayList<String>();
								
								for (String s:  t2._1()) {
									String[] l = s.split(" ");
									for (int i = 0; i < l.length; i++) {
										temp.add(l[i]);
									}
								}
								t1._1().addAll(temp);
								t1._1().addAll(t2._1());
								return new Tuple2<List<String>, Double>(t1._1(), t1._2() + t2._2());
							}
						});
		
		
		 JavaRDD<Tuple3<String, List<String>, Double>> final_stage =
				 mapping.map(new Function<Tuple2<String,Tuple2<List<String>, Double>>,
						 Tuple3<String, List<String>, Double>> () {
		  
		  @Override public Tuple3<String, List<String>, Double> call(
		  Tuple2<String, Tuple2<List<String>, Double>> input) throws Exception {
			  
			  		  
				  String url = input._1();
				  List<String> words = input._2()._1(); 
				  Double weight = input._2()._2(); 				  
				  String sourceURL   = url;
				  
			      String[] parsed = sourceURL.split("(//)|(/)|(\\.)");
			      List<String> allUsefulWords = new ArrayList<>();
			      for (String word: parsed) {
			          if (!ignore.contains(word)) {
			              allUsefulWords.add(word);
			          }
			      }		      	        		    
				  double urlScale = 0;
				  double urlBonus = 60;				  
				  for (String word: words) {	  
					  if (allUsefulWords.contains(word)) {
						  urlScale += 1.0 / allUsefulWords.size();
					  }
				  }			  
				  weight = weight + urlScale * urlBonus;		  
				  return new Tuple3<>(url, words, weight);  		  
		  		}
			})
		  
		  .sortBy(new Function< Tuple3<String, List<String>, Double>, Double>() {
		  
			  @Override public Double call(Tuple3<String, List<String>, Double> t) throws Exception {
				  
				  return t._3();
			  }
		  }, false,10);
		 
//		 final_stage.saveAsTextFile("temp");
		 		 		 
		 List<Tuple3<String, List<String>, Double>> res = new LinkedList<Tuple3<String, List<String>, Double>>(final_stage.collect());
		 return res;
	}
		
	
	public void stopSpark() {
		spark.stop();
	}
}
