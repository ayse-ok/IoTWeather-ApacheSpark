package com.apache.spark.IotWeather;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

public class App {
    public static void main( String[] args ) throws StreamingQueryException{
    	System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");
    	
    	SparkSession session = SparkSession.builder().appName("IotWeather").master("local").getOrCreate();
    	StructType weatherSchema = new StructType().add("quarter","string")
    		.add("heatType","string")
    		.add("heat","integer")
    		.add("windType","string")
    		.add("wind","integer");
    	
    	Dataset<Row> rawData = session.readStream().schema(weatherSchema).option("sep", ",").csv("F:\\GITHUB_PROJECTS\\BigDataTechWorkspace\\sparkstreaming\\*");  	    	    	
    	Dataset<Row> heatData = rawData.select("quarter","wind","heat").where("wind>20 AND heat>29");
    	    	
    	StreamingQuery start = heatData.writeStream().outputMode("append").format("console").start();
    	start.awaitTermination();
    }
}
