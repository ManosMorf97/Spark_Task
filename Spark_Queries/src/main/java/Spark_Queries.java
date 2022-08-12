import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.spark.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
//import java.io.FileReader;
import java.util.List;

public class Spark_Queries {
	
	public static void main(String[] args) {
		SparkSession session=SparkSession.
				builder()
				.appName("Spark_Queries")
				.master("local[*]")
				.getOrCreate();
		try(JavaSparkContext context=new 
				JavaSparkContext(session.sparkContext())){
			//ArrayList<Movie> movies=new ArrayList<Movie>();
			JavaRDD<String> movie_lines=context.
					textFile("ml-10m/ml-10M100K/movies.dat");
			JavaRDD<String> rating_lines=context.
					textFile("ml-10m/ml-10M100K/ratings.dat");
			JavaRDD<Movie> movies=movie_lines.map(
					line->createMovie(line));
			JavaPairRDD<String,Integer>pairs_movie_1=rating_lines.mapToPair( 
					line->new Tuple2 (takeMovieId(line),-1) ) ;
			JavaPairRDD<String,Integer>movie_views_rdd=pairs_movie_1.
					reduceByKey (( a , b)-> a + b) ;
			JavaPairRDD<Integer,String>views_movie_rdd=movie_views_rdd.
					mapToPair(mvr->new Tuple2(mvr._2(),mvr._1()));
			views_movie_rdd.sortByKey();
			views_movie_rdd.filter(p->true).top(25);
			//join with movies.dat
			//print results
		    //write your code
			
			
			
		}
	}
	public static String takeMovieId(String rating_line){
		List<String> attributes=Arrays.asList(rating_line.split("::"));
		return attributes.get(1);
	}
	public static Movie createMovie(String line) {
		List<String> attributes=Arrays.asList(line.split("::"));
		return new Movie(attributes.get(0),attributes.get(1),
				attributes.get(2));
	}
	
	public static void InsertMovies(ArrayList<Movie> movies) {
		try {
			FileReader fr=new FileReader("ml-10m/ml-10M100K/movies.dat");
			BufferedReader br=new BufferedReader(fr);
			String line=br.readLine();
			while (line!=null) {
				List<String> attributes=Arrays.asList(line.split("::"));
				Movie movie=new Movie(attributes.get(0),attributes.get(1),
						attributes.get(2));
				movies.add(movie);
				line=br.readLine();
			}
			br.close();
		if(!sorted(movies))
			Collections.sort(movies,Comparator.comparingInt(
					m -> m.MovieId_Number()));
		}catch(IOException e) {
			System.out.println("Insertion Failed");
			
		}
	}
	public static boolean sorted(ArrayList<Movie> movies) {
		for(int i=0; i<movies.size()-1; i++) {
			if(movies.get(i).MovieId_Number()>movies.get(i+1).MovieId_Number())
				return false;
		}
		return true;
	}


}

