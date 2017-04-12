package app.model;

import java.util.List;

import com.datastax.driver.mapping.annotations.Column;

import krug.daan.easynosql.cassandradb.annotation.SecondaryIndex;
import krug.daan.easynosql.cassandradb.dto.BaseDTO;

public class CassandraUser extends BaseDTO{
	
	@SecondaryIndex
	private String name;
	@SecondaryIndex
	private String email;
	@SecondaryIndex
	private Integer old;
	private java.util.Date created;
	private Long points;
	private Double rating;
	
	public CassandraUser(){
		super(CassandraUser.class);
	}

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
