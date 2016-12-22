package edu.upenn.cis455.indexer.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.upenn.cis455.indexer.utils.DownloadUtils;

public class IndexerHadoopMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static Logger log = Logger.getLogger(IndexerHadoopMapper.class);
	private static Set<String> stops = new HashSet<>();
	private static int count = 0;
	// regex filter to get only legal words
//	private static Pattern pan = Pattern.compile("[a-zA-Z0-9.@-]+");
	private static Pattern pan2 = Pattern.compile("[a-zA-Z]+");
	private static Pattern pan3 = Pattern.compile("[0-9]+,*[0-9]*");
	private static Pattern pan4 = Pattern.compile("^[a-zA-Z0-9]+[.@&-]*[a-zA-Z0-9]+");
	private static Set<String> bigramDict = new HashSet<>();
	private static Set<String> trigramDict = new HashSet<>();

	static {
		List<String> list = Arrays.asList(new String[] { "need", "also", "play", "feel", "felt", "want", "n't", "nt",
				"last", "take", "go", "get", "going", "long", "lot", "little", "first", "second", "third", "a", "about",
				"above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at",
				"be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot",
				"could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during",
				"each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having",
				"he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his",
				"how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's",
				"its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "n't",
				"of", "off", "on", "one", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out",
				"over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some",
				"such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there",
				"there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to",
				"too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
				"weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's",
				"whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're",
				"you've", "your", "yours", "yourself", "yourselves", "two", "three", "four", "five", "six", "seven",
				"eight", "nine", "ten", "na", "ago" });
		for (String word : list) {
			stops.add(word);
		}
		try {
			File file2 = new File("./Ngrams/dict2.txt");
			File file3 = new File("./Ngrams/dict3.txt");
			BufferedReader in = new BufferedReader(new FileReader(file2));
			String line = "";
			while ((line = in.readLine()) != null) {
				bigramDict.add(line);
			}
			in.close();
			in = new BufferedReader(new FileReader(file3));
			while ((line = in.readLine()) != null) {
				trigramDict.add(line);
			}
			in.close();
		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			log.error(sw.toString());
		}
	}

	public synchronized static void increment() {
		count++;
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {

		String line = value.toString(); // Doc ID from S3
		String keyName = line.trim();

		increment();
		// Date date = new Date();
		// SimpleDateFormat sdf = new SimpleDateFormat("E yyyy.MM.dd 'at'
		// hh:mm:ss a zzz");
		// sdf.setTimeZone(TimeZone.getTimeZone("EST"));
		// String nowDate = sdf.format(date);

		long start = System.currentTimeMillis();
		InputStream in = DownloadUtils.downloadfileS3(keyName);
		long end1 = System.currentTimeMillis();
		long end2 = 0;
		long end3 = 0;
		if (in != null) {
			Map<String, Integer> weights = new HashMap<>();
			Set<String> titles = new HashSet<>();
			int maxOccur = parse(in, keyName, weights, titles);
			end2 = System.currentTimeMillis();

			for (String word : weights.keySet()) {
				double tf = weights.get(word) * 1.0 / maxOccur;
				String val = String.format("%.5f", tf);
				if (titles.contains(word)) {
					val += ":T";
				}
				Text text = new Text(keyName + " " + val);
				Text wordText = new Text(word);
				context.write(wordText, text);
			}
			weights.clear();
			end3 = System.currentTimeMillis();
		}
		Runtime rt = Runtime.getRuntime();
		long usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;

		log.info("Mapper ----> " + line + " Count ---> " + count + " Download: " + (end1 - start) + "ms" + " parsing: "
				+ (end2 - end1) + "ms loop: " + (end3 - end2) + "ms" + " UsedMemory : " + usedMB + "MB");
	}

	public static int parse(InputStream in, String url, Map<String, Integer> weights, Set<String> titles)
			throws IOException {
		int legalWords = 0;
		int allWords = 0;
		int maxOccur = 1;
		try {
			org.jsoup.nodes.Document d = Jsoup.parse(in, "UTF-8", "");

			d.select(":containsOwn(\u00a0)").remove();
			Elements es = d.select("*");

			int NonEnglish = 4;

			for (Element e : es) {
				String nodeName = e.nodeName(), text = e.ownText().trim();

				// log.info(e.nodeName() + ": " + e.ownText());
				if (text != null && !text.isEmpty() && text.length() != 0) {
					edu.stanford.nlp.simple.Document tagContent = new edu.stanford.nlp.simple.Document(text);
					List<edu.stanford.nlp.simple.Sentence> sentences = tagContent.sentences();

					for (edu.stanford.nlp.simple.Sentence s : sentences) {
						// System.out.println("sentence:" + s);
						maxOccur = parseNgrams(s, weights, maxOccur,
								nodeName, titles);
						long time1 = System.currentTimeMillis();
						List<String> words = s.lemmas();
						long time2 = System.currentTimeMillis();

						if (time2 - time1 > 30) {
							int size = words.size();
							int dirties = 0;
							Matcher m;
							for (String w : words) {
								m = pan4.matcher(w);
								if (!m.matches())
									dirties++;
							}
							if (size != 0 && dirties * 1.0 / size > 0.5) {
								NonEnglish--;
								log.warn("Dirties Detected -- > " + words);
							}

							if (NonEnglish == 0) {
								weights.clear();
								titles.clear();
								log.fatal("NonEnglish Detected ---> URL Hashing: " + url);
								return 1;
							}
						}

						Matcher m, m2, m3;
						for (String w : words) {
							allWords++;
							w = w.trim(); // trim
							m = pan4.matcher(w);
							m2 = pan2.matcher(w);
							m3 = pan3.matcher(w);
							if (m.matches()) {
								if (m2.find()) {
									if (!w.equalsIgnoreCase("-rsb-") && !w.equalsIgnoreCase("-lsb-")
											&& !w.equalsIgnoreCase("-lrb-") && !w.equalsIgnoreCase("-rrb-")
											&& !w.equalsIgnoreCase("-lcb-") && !w.equalsIgnoreCase("-rcb-")) {
										w = w.toLowerCase();
										String value;
										if (!stops.contains(w)) {
											// all legal words must be indexed
											// with weight
											legalWords++;
											int weight = 1;
											if (nodeName.equalsIgnoreCase("title")) {
												// TODO
												// may need to be tuned
												titles.add(w);
												weight = 1;
											}
											if (!weights.containsKey(w)) {
												// collector.emit(new
												// Values<Object>(url, w));
												weights.put(w, weight);
											} else {
												int occur = weights.get(w) + weight;
												weights.put(w, occur);
												maxOccur = Math.max(maxOccur, occur);
											}
											// value = url + ":" + nodeName +
											// ":" + weight;
										}
										// else {
										// value = url;
										// }
										// System.out.println("key: " + w + "
										// value: " + value);
									}
								} else {
									// illegal word: extract number only - eg
									// 2014
									// only index but no weight
									if (m3.matches()) {
										// System.out.println("number:" + w);
										// String value = url;
									}
								}
							}
							// illegal word, extract numbers only - 36,000 ->
							// 36000
							// only index but no weight
							else {
								if (m3.matches()) {
									// w = w.replaceAll(",", "");
									// String value = url;
									// System.out.println("number:" + w);
								}
							}
						}
					}
				}
			}
			System.out.println("lemmas count ----> : " + count + "ms");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			log.fatal("url:" + url);
			// System.exit(0);
		} finally {
			in.close();
		}
		return maxOccur;
	}

	public static int parseNgrams(edu.stanford.nlp.simple.Sentence sentence, Map<String, Integer> weights, int maxOccur,
			String nodeName, Set<String> titles) {
		boolean isTitle = false;
		if (nodeName.equalsIgnoreCase("title")) {
			isTitle = true;
		}
		List<String> words = sentence.words();
		if ((words == null) || (words.size() == 0)) {
			return maxOccur;
		}
		String prefix2 = "";
		String prefix3 = "";
		for (int i = 0; i < words.size(); i++) {
			Matcher m = pan4.matcher((CharSequence) words.get(i));
			if (m.find()) {
				if (prefix2.length() != 0) {
					String candidate = prefix2 + " " + (String) words.get(i);
					if (bigramDict.contains(candidate)) {
						if (!weights.containsKey(candidate)) {
							weights.put(candidate, Integer.valueOf(1));
							if (isTitle) {
								titles.add(candidate);
							}
						} else {
							int val = ((Integer) weights.get(candidate)).intValue() + 1;
							weights.put(candidate, Integer.valueOf(val));
							maxOccur = Math.max(maxOccur, val);
						}
					}
				}
				prefix2 = (String) words.get(i);
				if (prefix3.contains(" ")) {
					String candidate = prefix3 + " " + (String) words.get(i);
					if (trigramDict.contains(candidate)) {
						if (!weights.containsKey(candidate)) {
							weights.put(candidate, Integer.valueOf(1));
							if (isTitle) {
								titles.add(candidate);
							}
						} else {
							int val = ((Integer) weights.get(candidate)).intValue() + 1;
							weights.put(candidate, Integer.valueOf(val));
							maxOccur = Math.max(maxOccur, val);
						}
					}
					prefix3 = candidate.substring(candidate.indexOf(' ') + 1);
				} else if (prefix3.length() != 0) {
					prefix3 = prefix3 + " " + (String) words.get(i);
				} else {
					prefix3 = (String) words.get(i);
				}
			} else {
				prefix2 = "";
				prefix3 = "";
			}
		}
		return maxOccur;
	}
}
