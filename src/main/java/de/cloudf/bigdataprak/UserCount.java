package de.cloudf.bigdataprak;

public class UserCount {
	private String username;
	private int ratingcount;
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public int getRatingcount() {
		return ratingcount;
	}
	public void setRatingcount(int ratingcount) {
		this.ratingcount = ratingcount;
	}
	public UserCount(String username, int ratingcount) {
		super();
		this.username = username;
		this.ratingcount = ratingcount;
	}
	
}
