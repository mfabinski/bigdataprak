package de.cloudf.bigdataprak;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * 
 * 
 * @author robert.euler
 * Ausgabe aller Filmtitel, die der Nutzer mit der ID = 10 bewertet hat.
 * 
 */
public class Aufgabe2 {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
			Text title = movieTitle(value.toString());
			if(title.getLength() != new Text("").getLength()) {
				context.write(title, new Text(""));
			}
			
			
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, new Text(""));
		}
	}
	public static Text movieTitle(String value) throws IOException{
		JSONParser parser = new JSONParser();
		try {
			JSONObject movie = (JSONObject) parser.parse(value);
			JSONArray ratings = (JSONArray) movie.get("ratings");	
			Iterator<JSONObject> iterator = ratings.iterator();
            while (iterator.hasNext()) {
                JSONObject rating = (JSONObject) iterator.next();
               if( Integer.parseInt(rating.get("userId").toString()) == 10) {
            	   		return new Text(movie.get("title").toString());    	   
               };
            }	
		} catch (ParseException e) {
			e.printStackTrace();
		} catch( Exception e) {
			e.printStackTrace();
			throw new IOException("Parsefehler...." + e.getMessage());
		}
		return new Text("");
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
