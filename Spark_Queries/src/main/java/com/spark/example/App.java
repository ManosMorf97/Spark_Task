package com.spark.example;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class App {
	public static Scanner scanner=new Scanner(System.in);
	
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
			movie_info=movie_info.persist(StorageLevel.MEMORY_AND_DISK( )) ;
			JavaRDD<String> rating_lines=context.
					textFile(path+"ml-10m/ml-10M100K/ratings.dat");
			rating_lines=rating_lines.persist(StorageLevel.DISK_ONLY( )) ;
			
			most_viewed_25_movies(context,movie_info,rating_lines);
			Good_Comedies(context,movie_info,rating_lines);
			top_10_romantic_movies_december(context,movie_info,rating_lines);
			
			
			scanner.close();
			
			//join with movies.dat
			//print results
		    //write your code
			context.close();
			
			
			
		}
	}
	
	public static void most_viewed_25_movies(JavaSparkContext context,
			JavaPairRDD<String,String> movie_info,JavaRDD<String> rating_lines) {
		
		JavaPairRDD<String,Integer>movie_1_rdd=rating_lines.mapToPair( 
				line->new Tuple2<String,Integer>(takeMovieId(line),1));
		JavaPairRDD<String,Integer> movie_views_rdd=movie_1_rdd.
				reduceByKey (( a , b)-> a + b);
		movie_views_rdd=movie_views_rdd.persist(StorageLevel.MEMORY_AND_DISK( ));
		JavaPairRDD<Integer,String> views_movie_rdd=movie_views_rdd.
				mapToPair(mvr->
					new Tuple2<Integer,String>(mvr._2(),mvr._1()));
		//views_movie_rdd=views_movie_rdd.persist(StorageLevel.MEMORY_AND_DISK( ));
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
		print_results(results,"The 25 most rated movies are","");
	}
	
	public static void  Good_Comedies(JavaSparkContext context,
			JavaPairRDD<String,String> movie_info,JavaRDD<String> rating_lines) {
		
		JavaPairRDD<String,String> comedies=movie_info.filter(
				movie->IsComedy(movie._2));
		comedies=comedies.persist(StorageLevel.MEMORY_AND_DISK( )) ;
		JavaPairRDD<String,String> user_ids_pair=rating_lines.mapToPair(
				rating->new Tuple2<String,String>(
						rating.substring(0,rating.indexOf(':'))," ")).
							reduceByKey((a,b)->" ");
		List<String> user_ids=user_ids_pair.collect().
				stream().
				map(res->res._1).
				collect(Collectors.toList());
		String user_id_input="";
		while(true) {
			System.out.println("Choose a user from 1 "
					+ "to " +user_ids.size()+" to see the comedies they love");
			user_id_input=scanner.nextLine();
			if(user_ids.contains(user_id_input)) break;
		}
		String user_id=user_id_input;
		JavaPairRDD<String,String> loved_movie_ids=rating_lines.filter(
				rating->rating.substring(0,rating.indexOf(':')).equals(user_id)&&
				Grade(rating)>=3.0).mapToPair(rating_line->
				new Tuple2<String,String>(takeMovieId(rating_line)," "));
		JavaPairRDD<String,Tuple2<String,String>>loved_comedies_rdd=
				loved_movie_ids.join(comedies);
		List<Tuple2<String,Tuple2<String,String>>> loved_comedies=
				loved_comedies_rdd.collect();
		print_results(loved_comedies,"User: "+user_id+" loves these comedies",
				"User: "+user_id+"does not love a comedy by this list");
	}
	
	public static void top_10_romantic_movies_december(
			JavaSparkContext context,JavaPairRDD<String,String> movie_info,
			JavaRDD<String> rating_lines) {
		
		JavaPairRDD<String,String> romance=movie_info.filter(
				movie->IsRomance(movie._2));
		romance=romance.persist(StorageLevel.MEMORY_AND_DISK( )) ;
		JavaPairRDD<String,Double> rated_december=rating_lines.filter(
				rating->rated_on_december(rating)).mapToPair(
						rating->new Tuple2<String,Double>(
								takeMovieId(rating),Grade(rating))).
				reduceByKey((a,b)->a+b);
		JavaPairRDD<String,Tuple2<Double,String>> december_romance=
				rated_december.join(romance);
		JavaPairRDD<Double,Tuple2<String,String>> 
		top_december_romance_rdd=
				december_romance.mapToPair(
						rdd->new Tuple2<Double,Tuple2<String,String>>(
								rdd._2._1,new Tuple2<String,String>(rdd._1,rdd._2._2))).
				sortByKey(false);
		List<Tuple2<Double,Tuple2<String,String>>> results=
				top_december_romance_rdd.take(10);
		List<Tuple2<String,Tuple2<String,String>>> results_fit_function=
				results.stream()
				.map(res->new Tuple2<String,Tuple2<String,String>>(
						res._2._1,new Tuple2<String,String>(res._2._1,res._2._2))).
				collect(Collectors.toList());
		print_results(results_fit_function,"Top 10 december romantic movies",
				"Sorry no romantic movies rated on December");		
	}
	
	public static void print_results(
			List<Tuple2<String,Tuple2<String,String>>> results,String msg,
			String msg_noRows) {
		
		List<String> results_2_2=results.stream()
				.map(res->res._2._2).
				collect(Collectors.toList());
		Collections.sort(results_2_2);
		System.out.println("---------------------------------");
		System.out.println(msg);
		if(results_2_2.isEmpty()) {
			System.out.println(msg_noRows + " Press any key to continue");
		}else {
			results_2_2.forEach(
					res->System.out.println(res));
			System.out.println("Done.Press any key to  continue");
		}
		scanner.nextLine();
	}
	
	public static String takeMovieId(String rating_line){
		List<String> attributes=Arrays.asList(rating_line.split("::"));
		return attributes.get(1);
	}
	
	public static double Grade(String rating_line) {
		List<String> attributes=Arrays.asList(rating_line.split("::"));
		return Double.parseDouble(attributes.get(2));
	}
	
	public static boolean IsComedy(String movie_info) {
		List<String> attributes=Arrays.asList(movie_info.split("  "));
		return attributes.get(2).contains("Comedy");
	}
	
	public static boolean IsRomance(String movie_info) {
		List<String> attributes=Arrays.asList(movie_info.split("  "));
		return attributes.get(2).contains("Romance");
	}
	
	public static boolean rated_on_december(String rating_line) {
		List<String> attributes=Arrays.asList(rating_line.split("::"));
		long timestamp=Long.parseLong(attributes.get(3));
		Timestamp ts = new Timestamp(timestamp);
		Date date = new Date(ts.getTime());
		return date.toString().contains(" Dec ");
	}

}

