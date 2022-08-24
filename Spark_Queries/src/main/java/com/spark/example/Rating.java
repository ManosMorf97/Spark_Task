package com.spark.example;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

public class Rating implements Serializable{
	private static final long serialVersionUID = 1L;
	private String userId;
	private String movieId;
	private float grade;
	private boolean december_rated;
	public Rating(String userId, String movieId, String grade,String timestamp){
		this.userId = userId;
		this.movieId = movieId;
		this.grade = Float.parseFloat(grade);
		december_rated=rated_on_december(timestamp);
	}
	
	public static boolean rated_on_december(String timestamp) {
		long timestamp_long=Long.parseLong(timestamp);
		Timestamp ts = new Timestamp(timestamp_long);
		Date date = new Date(ts.getTime());
		return date.toString().contains(" Dec ");
	}
	public String getUserId() {
		return userId;
	}
	public String getMovieId() {
		return movieId;
	}
	public float getGrade() {
		return grade;
	}
	
	public boolean getDecemberRated() {
		return december_rated;
	}
	

}
