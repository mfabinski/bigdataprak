package de.cloudf.bigdataprak;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import de.cloudf.bigdataprak.MovieRating;
/**
 *  4. Ausgabe aller Filme mit einer durchschnittlichen Bewertung >= 4.
 * @author matthias
 *
 */
public class Aufgabe4 {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {

		// Map all movies average rating
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			MovieRating result = averageRating(value.toString()); 
			context.write(new Text(result.getMovie()), new DoubleWritable(result.getRatingcount()));
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		
		// Filter with reduce method only those movies we need
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			
			for (DoubleWritable val : values) {
				// we only have one key per movie, so it is enough to check the first value.
				// filter movies with rating >= 4
				if (val.get() >= 4 ) {
					context.write(key, new DoubleWritable(val.get()));
				}
				return; 
			}
		
		}
	}

	public static MovieRating averageRating(String text) {
		JSONParser parser = new JSONParser();
		try {
			JSONObject movie = (JSONObject) parser.parse(text);
			JSONArray ratings = (JSONArray) movie.get("ratings");
			Object[] ratingArray = ratings.toArray();
			double averageRating = 0; int count = 0;
			for (Object r : ratingArray) {
				JSONObject rating = (JSONObject) r;
				averageRating += Double.parseDouble(rating.get("rating").toString());
				count++;
			}
			if (count>0) {
				return new MovieRating((String)movie.get("title"), averageRating / count);
			} else {
				return new MovieRating((String)movie.get("title"), 0);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return new MovieRating("", 1);
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
