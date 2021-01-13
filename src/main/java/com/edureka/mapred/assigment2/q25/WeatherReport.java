package com.edureka.mapred.assigment2.q25;

public class WeatherReport {
	private String date;
	private Double avgTemp;
	private Double avgTempUncertainity;
	private String city;
	private String country;
	private String latitude;
	private String longitude;
	
	public WeatherReport(String date, Double avgTemp, Double avgTempUncertainity, String city, String country,
			String latitude, String longitude) {
		this.date = date;
		this.avgTemp = avgTemp;
		this.avgTempUncertainity = avgTempUncertainity;
		this.city = city;
		this.country = country;
		this.latitude = latitude;
		this.longitude = longitude;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public Double getAvgTemp() {
		return avgTemp;
	}

	public void setAvgTemp(Double avgTemp) {
		this.avgTemp = avgTemp;
	}

	public Double getAvgTempUncertainity() {
		return avgTempUncertainity;
	}

	public void setAvgTempUncertainity(Double avgTempUncertainity) {
		this.avgTempUncertainity = avgTempUncertainity;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getLatitude() {
		return latitude;
	}

	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}

	public String getLongitude() {
		return longitude;
	}

	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}

	@Override
	public String toString() {
		return date + "," + avgTemp + "," + avgTempUncertainity + "," + city + ","
				+ country + "," + latitude + "," + longitude + "\n";
	}
	
	

}
