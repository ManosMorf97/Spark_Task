package com.spark.example;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class App {
	public static void main(String[] args) {
		System.out.println("Ready");
		String path="/home/manos/Μεταπτυχιακό/1o Εξάμηνο/"
				+ "Συστήματα Διαχείρησης Δεδομένων Μεγάλης Κλίμακας/"
				+ "Εργασία/Spark_Queries/";
		SparkSession session=SparkSession.
				builder()
				.appName("Spark_Queries")
				.master("local[*]")
				.getOrCreate();
		try(JavaSparkContext context=new 
				JavaSparkContext(session.sparkContext())){
			//ArrayList<Movie> movies=new ArrayList<Movie>();
			JavaRDD<String> movie_lines=context.
					textFile(path+"ml-10m/ml-10M100K/movies.dat");
			JavaPairRDD<String,String> movie_info=movie_lines.mapToPair(
					line->new Tuple2<String,String>(
							line.substring(0,line.indexOf(':')),
								line.replace(':',' ')));
			movie_info=movie_info.persist(StorageLevel.DISK_ONLY( )) ;
			JavaRDD<String> rating_lines=context.
					textFile(path+"ml-10m/ml-10M100K/ratings.dat");
		
			
			
			JavaPairRDD<String,Integer>movie_1_rdd=rating_lines.mapToPair( 
					line->new Tuple2<String,Integer>(takeMovieId(line),1));
			JavaPairRDD<String,Integer> movie_views_rdd=movie_1_rdd.
					reduceByKey (( a , b)-> a + b);
			movie_views_rdd=movie_views_rdd.persist(StorageLevel.DISK_ONLY( ));
			JavaPairRDD<Integer,String> views_movie_rdd=movie_views_rdd.
					mapToPair(mvr->
						new Tuple2<Integer,String>(mvr._2(),mvr._1()));
			//views_movie_rdd=views_movie_rdd.persist(StorageLevel.DISK_ONLY( ));
			JavaPairRDD<Integer,String>sorted_vmrdd=views_movie_rdd.
					sortByKey(false);
			List<String> top_movies=sorted_vmrdd.take(25).stream().
					map(res->res._2).
					collect(Collectors.toList());
			JavaPairRDD<String,String> top_movies_rdd=
					context.parallelize(top_movies).mapToPair(
							id->new Tuple2<String,String>(id," "));
			JavaPairRDD<String,Tuple2<String,String>>results_rdd=
					top_movies_rdd.join(movie_info);
			List<Tuple2<String,Tuple2<String,String>>> results=
					results_rdd.take(25);
			List<String> results_2_2=(ArrayList<String>)results.stream()
					.map(res->res._2._2).
					collect(Collectors.toList());
			Collections.sort(top_movies);
			Collections.sort(results_2_2);
			System.out.println("---------------------------------");
			System.out.println("Top 25 most viewed movies");
			results_2_2.forEach(
					res->System.out.println(res));
			top_movies.forEach(
					movie->System.out.println(movie));
			System.out.println("Done.Press any key to  continue");
			Scanner scanner=new Scanner(System.in);
			scanner.nextLine();
			scanner.close();
			//join with movies.dat
			//print results
		    //write your code
			context.close();
			
			
			
		}
	}
	public static String takeMovieId(String rating_line){
		List<String> attributes=Arrays.asList(rating_line.split("::"));
		return attributes.get(1);
	}


}

