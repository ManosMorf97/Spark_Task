package com.spark.example;

import org.apache.spark.api.java.JavaRDD;

import java.util.Scanner;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TestApp {


	public static void main(String[] args) {
		Scanner scanner=new Scanner(System.in);
		String path="/home/manos/Μεταπτυχιακό/1o Εξάμηνο/"
				+ "Συστήματα Διαχείρησης Δεδομένων Μεγάλης Κλίμακας/"
				+ "Εργασία/Spark_Queries/";
		SparkSession session=SparkSession.
				builder()
				.appName("Spark_Queries")
				.master("local[*]")
				.getOrCreate();
		String movies_path=path+"ml-10m/ml-10M100K/movies.dat";
		String ratings_path=path+"ml-10m/ml-10M100K/ratings.dat";
		JavaRDD<Movie> movies=session.read().textFile(movies_path).
				javaRDD().map(line->{
					String [] parts=line.split("::");
					return new Movie(parts[0],parts[1],parts[2]);
				});
		Dataset<Row> movies_df=session.createDataFrame(movies, Movie.class);
		movies_df.createOrReplaceGlobalTempView("movies");
		
		JavaRDD<Rating> ratings=session.read().textFile(ratings_path).
				javaRDD().map(line->{
					String [] parts=line.split("::");
					return new Rating (parts[0],parts[1],parts[2],parts[3]);
				});
		Dataset<Row> ratings_df=session.createDataFrame(ratings, Rating.class);
		ratings_df.createOrReplaceGlobalTempView("ratings");
		Dataset<Row> joined=session.sql("select global_temp.ratings.userId,"
				+ "global_temp.ratings.movieId"
				+ ",global_temp.ratings.grade,global_temp.ratings.decemberRated,"
				+ "global_temp.movies.title,"
				+ "global_temp.movies.genres"
				+ " from global_temp.ratings"
				+ " inner join global_temp.movies on"
				+ " global_temp.movies.movieId=global_temp.ratings.movieId");
		joined.createOrReplaceGlobalTempView("joined_table");
		
		Dataset<Row> views_ammount=session.sql("select movieId,title,genres,"
				+ "count(*) as viewers from global_temp.joined_table group By"
				+ " movieId,title,genres");
		views_ammount.createOrReplaceGlobalTempView("views_ammount");
		Dataset<Row> query1=session.sql("select * from global_temp."
				+ "views_ammount order by"
				+ " viewers desc");
		query1.show(25);
		
		scanner.nextLine();
		
		Dataset<Row> query2=session.sql("select count(movieId)"
				+ " from global_temp.joined_table where userId='1' "
				+ "AND genres like '%Comedy%' "
				+ " AND grade>=30");
		query2.show();
		
		scanner.nextLine();
		
		Dataset<Row> query3=session.sql("select movieId,title,genres,"
				+ "avg(grade) as av_grade from global_temp.joined_table "
				+ "where decemberRated=True AND genres like '%Romance%' "
				+ " group By movieId,title,genres order by( av_grade) desc");
		query3.show(10);
		
		scanner.nextLine();
		
		Dataset<Row> views_december=session.sql("select movieId,title,genres,"
				+ "count(movieId) as viewers from global_temp.joined_table "
				+ "where decemberRated==True"
				+ " group By movieId,title,genres order by (viewers) desc");
		views_december.createOrReplaceGlobalTempView("d_views");
		Dataset<Row> query4=session.sql("select * from global_temp.d_views "
				+ "where viewers="
				+ "(select viewers from global_temp.d_views limit(1))");
		query4.show();
		
		scanner.nextLine();
		scanner.close();
		
		
		

	}

}
