package com.spark.example;


import java.util.Arrays;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import java.util.List;
import java.util.Scanner;

public class App {

	public static void main(String[] args) {
		System.out.println("Ready");
		String path="/home/manos/Μεταπτυχιακό/1o Εξάμηνο/"
				+ "Συστήματα Διαχείρησης Δεδομένων Μεγάλης Κλίμακας/"
				+ "Εργασία/Spark_Queries/";
		SparkSession session=SparkSession.
				builder()
				.appName("Spark_Queries")
				.master("local[4]")
				.getOrCreate();
		try(JavaSparkContext context=new 
				JavaSparkContext(session.sparkContext())){
			System.out.println("Top 25 most viewed movies");
			//ArrayList<Movie> movies=new ArrayList<Movie>();
			JavaRDD<String> movie_lines=context.
					textFile(path+"ml-10m/ml-10M100K/movies.dat");
			movie_lines=movie_lines.persist(StorageLevel.MEMORY_ONLY( )) ;
			JavaRDD<String> rating_lines=context.
					textFile(path+"ml-10m/ml-10M100K/ratings.dat");
			//JavaRDD<Movie> movies=movie_lines.map(
				//	line->createMovie(line));
			JavaPairRDD<String,Integer>movie_1_rdd=rating_lines.mapToPair( 
					line->new Tuple2<String,Integer>(takeMovieId(line),1));
			JavaPairRDD<String,Integer> movie_views_rdd=movie_1_rdd.
					reduceByKey (( a , b)-> a + b);
			JavaPairRDD<Integer,String> views_movie_rdd=movie_views_rdd.
					mapToPair(mvr->
					new Tuple2<Integer,String>(mvr._2(),mvr._1()));
			JavaPairRDD<Integer,String>sorted_vmrdd=views_movie_rdd.
					sortByKey(false);
			JavaPairRDD<String,Integer>sorted_mvrdd=sorted_vmrdd.mapToPair(
					s_vmrdd->new Tuple2<String,Integer>(
							s_vmrdd._2(),s_vmrdd._1()));
			sorted_mvrdd=sorted_mvrdd.persist(StorageLevel.MEMORY_ONLY( )) ;
			JavaPairRDD<String,String> movie_info=movie_lines.mapToPair(
					line->new Tuple2<String,String>(
							line.substring(line.indexOf(':')),
								line.replace(':',' ')));
			JavaPairRDD<String, Tuple2<Integer,String>> most_viewed =
					sorted_mvrdd.join(movie_info);
			List<Tuple2<String, Tuple2<Integer, String>>> results = 
				most_viewed.take(25);
			results.forEach(
					res->System.out.println("------------------------"));
			System.out.println("Done.Press any key to  continue");
			String end=new Scanner(System.in).nextLine();
			System.out.println(end);
			//join with movies.dat
			//print results
		    //write your code
			
			
			
		}
	}
	public static String takeMovieId(String rating_line){
		List<String> attributes=Arrays.asList(rating_line.split("::"));
		return attributes.get(1);
	}


}

