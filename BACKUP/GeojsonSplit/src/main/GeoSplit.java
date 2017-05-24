package main;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Scanner;

import com.alibaba.fastjson.JSON;

public class GeoSplit {
	
	private String type;
	private String id;
	private String properties;
	
	GeoSplit() {
		type = "";
		id = "";
		properties = "";
	}

	public static void main(String[] args) {
		

		try {
			FileReader fin = new FileReader("src/map.geojson");
			Scanner text = new Scanner(fin);
			String jsonString = text.nextLine();
			GeoSplit group = JSON.parseObject(jsonString, GeoSplit.class);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		

	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getProperties() {
		return properties;
	}

	public void setProperties(String properties) {
		this.properties = properties;
	}

}
