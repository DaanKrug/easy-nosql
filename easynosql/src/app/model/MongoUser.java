package app.model;

import krug.daan.easynosql.mongodb.dto.BaseDTO;

public class MongoUser extends BaseDTO{
	
	public MongoUser(){
		super(MongoUser.class);
	}
	
	private String name;
	private String email;
	private Integer old;
	private java.util.Date created;
	private Long points;
	private Double rating;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	public Integer getOld() {
		return old;
	}
	public void setOld(Integer old) {
		this.old = old;
	}
	public java.util.Date getCreated() {
		return created;
	}
	public void setCreated(java.util.Date created) {
		this.created = created;
	}
	public Long getPoints() {
		return points;
	}
	public void setPoints(Long points) {
		this.points = points;
	}
	public Double getRating() {
		return rating;
	}
	public void setRating(Double rating) {
		this.rating = rating;
	}
	

}
