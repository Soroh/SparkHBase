package defaultpackage;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.streaming.Durations;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class FinalGlobalMoodSparkHBAse {

	private static final String TABLE_NAME = "Global_mood";
	private static final String CF_MOOD = "Mood Details";
	private static final String CF_KEY_VARIABLES = "Key Variables";
	private static final String CF_ROW_KEY = "Row Key";

	public static void main(String[] args) throws IOException {

		Configuration conf1 = new Configuration();

		FileSystem fs = FileSystem.get(conf1);

		

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(
				"NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				Durations.seconds(10));
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream(
				"localhost", 9999);

		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x
				.split(" ")));

		JavaPairDStream<String, Integer> pairs = words
				.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairDStream<String, Integer> wordCounts = pairs
				.reduceByKey((i1, i2) -> i1 + i2);

		Set<String> happySet = new HashSet<>();
		happySet.add("love");
		happySet.add("loves");
		happySet.add("happy");
		happySet.add("enjoying");
		happySet.add("relazing");
		happySet.add("excited");
		Set<String> sadSet = new HashSet<>();
		sadSet.add("sad");
		sadSet.add("hate");
		sadSet.add("hates");
		sadSet.add("angry");
		sadSet.add("annoyed");
		sadSet.add("irritating");

		
		Map<String, Integer> nums = new HashMap<>();
		wordCounts.foreachRDD(frdd -> {
			frdd.collect().forEach(t -> {
				String s = t._1;
				Integer k = t._2;
				if (happySet.contains(s)) {
					nums.put("Happy Mood", k);
				} else if (sadSet.contains(s)) {
					nums.put("Sad Mood", k);
				}
			});
		});

		for (String s : nums.keySet()) {
			System.out.println(s + "\t " + nums.get(s));
		}
		wordCounts.print();
		

		wordCounts
				.foreachRDD(f -> {
					f.collect()
							.forEach(
									k -> {

										String s = k._1;
										int count = k._2;
										if (happySet.contains(s)) {
											Configuration config = HBaseConfiguration
													.create();
											try {
												try (Connection connection = ConnectionFactory
														.createConnection(config);
														Admin admin = connection
																.getAdmin()) {
													HTableDescriptor table = new HTableDescriptor(
															TableName
																	.valueOf(TABLE_NAME));
													table.addFamily(new HColumnDescriptor(
															CF_MOOD)
															.setCompressionType(Algorithm.NONE));
													table.addFamily(new HColumnDescriptor(
															CF_KEY_VARIABLES));
													table.addFamily(new HColumnDescriptor(
															CF_ROW_KEY));
													

													try {
														if (!admin.tableExists(table.getTableName())) {
															System.out
															.print("Creating table.... ");
															admin.createTable(table);
															System.out.println(" Table Created Successfully!");
														}
													} catch (Exception e1) {
														e1.printStackTrace();
													}
													
													

													// instantiate HTable class
													HTable hTable = null;
													try {
														hTable = new HTable(
																config,
																TABLE_NAME);
													} catch (Exception e1) {
														
														e1.printStackTrace();
													}
													Put p = new Put(Bytes
															.toBytes("row" + s));

													p.add(Bytes
															.toBytes(CF_MOOD),
															Bytes.toBytes("Mood"),
															Bytes.toBytes("Happy"));
													p.add(Bytes
															.toBytes(CF_MOOD),
															Bytes.toBytes("Count"),
															Bytes.toBytes(String
																	.valueOf(count)));

													try {
														hTable.put(p);
													} catch (Exception e) {

														e.printStackTrace();
													}

													try {
														hTable.close();
													} catch (Exception e) {
														
														e.printStackTrace();
													}
												}

											} catch (Exception e) {
											}
										} else if (sadSet.contains(s)) {
											Configuration config = HBaseConfiguration
													.create();
											try {
												try (Connection connection = ConnectionFactory
														.createConnection(config);
														Admin admin = connection
																.getAdmin()) {
													HTableDescriptor table = new HTableDescriptor(
															TableName
																	.valueOf(TABLE_NAME));
													table.addFamily(new HColumnDescriptor(
															CF_MOOD)
															.setCompressionType(Algorithm.NONE));
													table.addFamily(new HColumnDescriptor(
															CF_KEY_VARIABLES));
													table.addFamily(new HColumnDescriptor(
															CF_ROW_KEY));
													try {
														if (!admin.tableExists(table.getTableName())) {
															System.out
															.print("Creating table.... ");
															admin.createTable(table);
															System.out.println(" Table Created Successfully!");
														}
													} catch (Exception e1) {
														
														e1.printStackTrace();
													}

													
													HTable hTable = null;
													try {
														hTable = new HTable(
																config,
																TABLE_NAME);
													} catch (Exception e1) {
														
														e1.printStackTrace();
													}
													
	Put p = new Put(Bytes
															.toBytes("row" + s));

													p.add(Bytes
															.toBytes(CF_MOOD),
															Bytes.toBytes("Mood"),
															Bytes.toBytes("Sad Mood"));
													p.add(Bytes
															.toBytes(CF_MOOD),
															Bytes.toBytes("Count"),
															Bytes.toBytes(String
																	.valueOf(count)));

													try {
														hTable.put(p);
													} catch (Exception e) {

														e.printStackTrace();
													}

													try {
														hTable.close();
													} catch (Exception e) {
														// TODO Auto-generated
														// catch block
														e.printStackTrace();
													}
												}

											} catch (Exception e) {
											}

										}
									});
				});

		jssc.start(); // Start the computation
		jssc.awaitTermination(); // Wait for the computation to terminate

	}
}