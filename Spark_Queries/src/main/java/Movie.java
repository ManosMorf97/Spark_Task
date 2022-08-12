/**
 * 
 */
//package com.spark.example;

import java.io.Serializable;


/**
 * @author manos
 *
 */
public class Movie implements Serializable{
	public String movieId;
	public String title;
	public String genres;

	/**
	 * 
	 */
	public Movie() {
		
	}
	public Movie(String movieId, String title, String genres) {
		this.movieId = movieId;
		this.title = title;
		this.genres = genres;
	}
	public String getMovieId() {
		return movieId;
	}

	public String getTitle() {
		return title;
	}

	public String getGenres() {
		return genres;
	}
	
	public int MovieId_Number() {
		return Integer.parseInt(movieId);
	}

}
