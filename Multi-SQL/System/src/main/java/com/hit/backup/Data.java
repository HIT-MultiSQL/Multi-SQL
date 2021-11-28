package com.hit.backup;

public class Data {
	private String name;
	private String datatype;
	private String type;
	private String introduction;
	public Data(String name, String datatype, String type, String introduction) {
		super();
		this.name = name;
		this.datatype = datatype;
		this.type = type;
		this.introduction = introduction;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDatatype() {
		return datatype;
	}
	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getIntroduction() {
		return introduction;
	}
	public void setIntroduction(String introduction) {
		this.introduction = introduction;
	}
	
}
