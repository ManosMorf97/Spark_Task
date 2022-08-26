package com.spark.example;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import static org.apache.spark.sql.functions.col;


import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class App {
	public static Scanner scanner=new Scanner(System.in);
	
	public static void main(String[] args) throws InterruptedException {
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
			String movies_path=path+"ml-10m/ml-10M100K/movies.dat";
			String ratings_path=path+"ml-10m/ml-10M100K/ratings.dat";
			JavaRDD<String> movie_lines=context.
					textFile(movies_path);
			JavaPairRDD<String,String> movie_info=movie_lines.mapToPair(
					line->new Tuple2<String,String>(
							line.substring(0,line.indexOf(':')),
								line.replace(':',' ')));
			movie_info=movie_info.persist(StorageLevel.MEMORY_AND_DISK( )) ;
			JavaRDD<String> rating_lines=context.
					textFile(ratings_path);
			JavaPairRDD<String,String> rating_info=rating_lines.mapToPair(
					line->new Tuple2<String,String>(
							takeMovieId(line),line.replace(":"," ")
							)
					);
			System.out.println("A BIG join is on the way.This could take a while");
			System.out.println("Loading");
			JavaPairRDD<String,Tuple2<String,String>> joined=rating_info.
					join(movie_info);
			JavaPairRDD<String,String>	joined_info=joined.
					mapToPair(tuple->new Tuple2<String,String>(
							tuple._1,tuple._2._1+"   "+tuple._2._2)
					);
			joined_info=joined_info.persist(StorageLevel.DISK_ONLY( )) ;
			
			most_viewed_25_movies(context,joined_info);
			Good_Comedies(context,joined_info);
			top_10_romantic_movies_december(context,joined_info);
			
			dataframes(session,movies_path,ratings_path);
			
			
			scanner.close();
			
			//join with movies.dat
			//print results
		    //write your code
			context.close();
			
			
			
		}
	}
	
	public static void most_viewed_25_movies(JavaSparkContext context,
			JavaPairRDD<String,String> joined_info) {
		
		JavaPairRDD<String,Long>movie_1_rdd=joined_info.mapToPair( 
				tuple->new Tuple2<String,Long>(
					movie_info(tuple._2),(long)1));
		JavaPairRDD<String,Long> movie_views_rdd=movie_1_rdd.
				reduceByKey (( a , b)-> a + b);
		JavaPairRDD<Long,String> views_movie_rdd=movie_views_rdd.
				mapToPair(mvr->
					new Tuple2<Long,String>(mvr._2(),mvr._1()));
		JavaPairRDD<Long,String>sorted_vmrdd=views_movie_rdd.
				sortByKey(false);
		JavaRDD<String> top_movies=sorted_vmrdd.map(tuple->tuple._2);
		List<String> results=top_movies.take(25);
		print_results(results,"The 25 most rated movies are","");
	}
	
	public static void  Good_Comedies(JavaSparkContext context,
			JavaPairRDD<String,String> joined_info) {
		
		JavaPairRDD<String,String> user_ids_pair=joined_info.mapToPair(
				rating->new Tuple2<String,String>(
						rating._2.substring(0,rating._2.indexOf("  "))," "));
		JavaPairRDD<String,String> users_ids_reduced=
						user_ids_pair.reduceByKey((a,b)->" ");
		List<String> user_ids=users_ids_reduced.collect().
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
		Function<Tuple2<String, String>, Boolean> loved_user_comedies=
				joined->(joined._2.substring(0,joined._2.indexOf("  ")).equals(user_id)&&
				Grade(joined._2)>=30&&IsComedy(joined._2));
		JavaPairRDD<String,String> loved_comedies_rdd=joined_info.filter(
				loved_user_comedies);
		List<Tuple2<String,String>> loved_comedies=loved_comedies_rdd.collect();
		System.out.println("---------------------------------");
		if(loved_comedies.isEmpty())
			System.out.println("User: "+user_id+"does not love a comedy by this list");
		else
			System.out.println("User: "+user_id+" loves "+loved_comedies.size()
					+ " comedies");
		System.out.println("Done.Press any key to  continue");
		scanner.nextLine();
		
		
	}
	
	public static void top_10_romantic_movies_december(
			JavaSparkContext context,JavaPairRDD<String,String> joined_info) {
		Function<Tuple2<String, String>, Boolean> december_romance=
		joined->(rated_on_december(joined._2)&&IsRomance(joined._2));	
		JavaPairRDD<String,String> rated_december_romance=joined_info.
				filter( december_romance);
		JavaPairRDD<String,Tuple2<Long,Long>> grade_tuple=
				rated_december_romance.mapToPair(pair->
				new Tuple2<String,Tuple2<Long,Long>>(movie_info(pair._2),
						new Tuple2<Long,Long>(Grade(pair._2),(long)1)));
		JavaPairRDD<String,Tuple2<Long,Long>> total_grade=grade_tuple.
				reduceByKey((a,b)->sum(a,b));
		JavaPairRDD<String,Double> average_grade= total_grade.mapToPair(tuple->
				new Tuple2<String,Double>(tuple._1,(double)(tuple._2._1/tuple._2._2)));
		
		JavaPairRDD<Double,String>movies_average_grade=average_grade.
				mapToPair(
				tuple->new Tuple2<Double,String>(tuple._2,tuple._1));
		JavaPairRDD<Double,String> sorted_pairs=movies_average_grade.
				sortByKey(false);
		JavaRDD<String>sorted_rdd=sorted_pairs.map(rdd->rdd._2);
		List<String> results=sorted_rdd.take(10);
		print_results(results,"Top 10 december romantic movies",
				"Sorry no romantic movies rated on December");		
	}
	
	public static void print_results(
			List<String> results,String msg,
			String msg_noRows) {
		
		System.out.println("---------------------------------");
		System.out.println(msg);
		if(results.isEmpty()) {
			System.out.println(msg_noRows + " Press any key to continue");
		}else {
			results.forEach(
					res->System.out.println(res));
			System.out.println("Done.Press any key to  continue");
		}
		scanner.nextLine();
	}
	
	public static String takeMovieId(String rating_line){
		List<String> attributes=Arrays.asList(rating_line.split("::"));
		return attributes.get(1);
	}
	
	public static long Grade(String rating_line) {
		List<String> attributes=Arrays.asList(rating_line.split("  "));
		return (long)(Double.parseDouble(attributes.get(2))*10);
	}
	
	public static boolean IsComedy(String joined_info) {
		String movie=movie_info(joined_info);
		List<String> attributes=Arrays.asList(movie.split("  "));
		return attributes.get(2).contains("Comedy");
	}
	
	public static boolean IsRomance(String joined_info) {
		String movie=movie_info(joined_info);
		List<String> attributes=Arrays.asList(movie.split("  "));
		return attributes.get(2).contains("Romance");
	}
	
	public static boolean rated_on_december(String rating_line) {
		List<String> attributes=Arrays.asList(rating_line.split("  "));
		return Rating.rated_on_december(attributes.get(3));
	}
	public static String movie_info(String line) {
		return line.substring(line.indexOf("   ")+3);
	}
	
	public static Tuple2<Long,Long> sum(Tuple2<Long,Long> a,
			Tuple2<Long,Long> b){
		
		return new Tuple2<Long,Long>(a._1+b._1,a._2+b._2);
	}
	
	
	public static void dataframes(SparkSession session,
			String movies_path,String ratings_path) 
					throws InterruptedException {
		
			System.out.println("Now we will use Dataframes");
			Thread.sleep(2);
			
			JavaRDD<Movie> movies=session.read().textFile(movies_path).
					javaRDD().map(line->{
						String [] parts=line.split("::");
						return new Movie(parts[0],parts[1],parts[2]);
					});
			Dataset<Row> movies_df=session.createDataFrame(movies, Movie.class);
			movies_df=movies_df.persist(StorageLevel.MEMORY_AND_DISK());
			
			JavaRDD<Rating> ratings=session.read().textFile(ratings_path).
					javaRDD().map(line->{
						String [] parts=line.split("::");
						return new Rating (parts[0],parts[1],parts[2],parts[3]);
					});
			Dataset<Row> ratings_df=session.createDataFrame(ratings, Rating.class);
			Dataset<Row> joined=ratings_df.join(movies_df,"movieId");
			joined=joined.persist(StorageLevel.DISK_ONLY( )) ;
			
			
			
			
			Dataset<Row> views=joined.groupBy("movieId","title","genres").count();
			Dataset<Row> first_25=views.orderBy(col("count").desc());
			System.out.println("Top 25 movies");
			first_25.select("movieId","title","genres").show(25);
			
			System.out.println("Done Press any key to continue");
			scanner.nextLine();
			
			Dataset<Row> users=joined.groupBy("userId").count();
			String user_id_input="";
			while(true) {
				System.out.println("Choose a user from 1 "
						+ "to " +users.count()+" to see the comedies they love");
				user_id_input=scanner.nextLine();
				if(joined.filter(col("userId").
						equalTo(user_id_input)).count()>0) break;
			}
			String user_id=user_id_input;
			
			Dataset<Row> user_movies=joined.filter(col("userId").
					like(user_id));
			Dataset<Row> comedies=user_movies.filter(col("genres").
					like("%Comedy%"));
			Dataset<Row>loved_comedies=comedies.filter(col("grade").
					gt(25));
			
			if (loved_comedies.count()==0)
				System.out.println("User: "+user_id+"does not love a comedy by this list");
			else
				System.out.println("User: "+user_id+" loves "+loved_comedies.count()
						+ " comedies");
			
			System.out.println("Done Press any key to continue");
			scanner.nextLine();
			
			Dataset<Row> rated_on_december=joined.filter(col("decemberRated"));
			rated_on_december=rated_on_december.persist(
					StorageLevel.MEMORY_AND_DISK( )) ;
			Dataset<Row> december_romantic=rated_on_december
					.filter(col("genres").like("%Romance%"));
			Dataset<Row> total_grade=december_romantic
					.groupBy("movieId","title","genres").avg("grade");
			Dataset<Row> top_10_romantic_movies=total_grade.orderBy(
					col("avg(grade)").desc());
			if(top_10_romantic_movies.count()==0)
				System.out.println("Sorry no romantic movies rated on December");
			else
				top_10_romantic_movies.show(10);
			
			System.out.println("Done.Press any key to continue");
			scanner.nextLine();
			
			Dataset<Row> movie_viewers=rated_on_december
					.groupBy("movieId","title","genres").count();
			if(movie_viewers.count()==0) {
				 System.out.println("No movies rated_on_december");
				 System.out.println("Press any key to continue");
				 scanner.nextLine();
				 System.exit(0);
			}
			Dataset<Row> movie_viewers_sorted=movie_viewers.orderBy(
					col("count").desc());
		  Row most_views=movie_viewers_sorted.select("count").first();
		  long most_views_val=most_views.getLong(0);
		 Dataset<Row> most_viewed=movie_viewers_sorted.
				 filter(col("count").geq(most_views_val));
		 if(most_viewed.count()==0)
			 System.out.println("No movies rated_on_december");
		  most_viewed.select("movieId","title","genres").show();
		  
			System.out.println("Done Press any key to continue");
			scanner.nextLine();
		    		
		}

}

