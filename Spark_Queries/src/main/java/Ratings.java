import java.util.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class Ratings {
	private String userId;
	private String movieId;
	private float grade;
	private Date timestamp;
	public Ratings(String userId, String movieId, String grade,String timestamp){
		this.userId = userId;
		this.movieId = movieId;
		this.grade = Float.parseFloat(grade);
		this.timestamp=new Date(new Timestamp(Long.parseLong(timestamp))
				.getTime());
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
	public Date getTimestamp() {
		return timestamp;
	}
	

}
