package de.cloudf.bigdataprak;

import java.io.IOException;
import java.util.Vector;

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
 *  Ausgabe aller Filmtitel, die der Nutzer mit der ID = 10 bewertet hat
 * @author matthias
 *
 */
public class Aufgabe3 {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Vector<UserCount> users = ratingCount(value.toString());
			for (UserCount uc : users) {
				context.write(new Text(uc.getUsername()), new IntWritable(uc.getRatingcount()));
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static Vector<UserCount> ratingCount(String text) {
		Vector<UserCount> uc = new Vector<UserCount>();
		JSONParser parser = new JSONParser();
		try {
			JSONObject movie = (JSONObject) parser.parse(text);
			JSONArray ratings = (JSONArray) movie.get("ratings");
			Object[] ratingArray = ratings.toArray();
			for (Object r : ratingArray) {
				JSONObject rating = (JSONObject) r;
				uc.add(new UserCount(""+ rating.get("userId"),1));
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return uc;
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
