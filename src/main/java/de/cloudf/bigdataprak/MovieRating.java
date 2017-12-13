package de.cloudf.bigdataprak;

public class MovieRating {
	private String movie;
	private double ratingcount;
	public String getMovie() {
		return movie;
	}
	public void setMovie(String movie) {
		this.movie = movie;
	}
	public double getRating() {
		return ratingcount;
	}
	public void setRatingcount(double ratingcount) {
		this.ratingcount = ratingcount;
	}
	public MovieRating(String movie, double ratingcount) {
		super();
		this.movie = movie;
		this.ratingcount = ratingcount;
	}
	
}
