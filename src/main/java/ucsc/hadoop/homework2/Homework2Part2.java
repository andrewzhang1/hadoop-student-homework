package ucsc.hadoop.homework2;

/**
 * Author: Andrew Zhang
 * Date:   Aug. 30, 2014
 * Purpose: 
 * 		Develop a MapReduce to show which actors played in each movie.
 * 		The input data format for the imdb.tsv (each actor played in the movies...):
 * 			
 *      McClure, Marc (I)       Freaky Friday   2003
 *		McClure, Marc (I)       Coach Carter    2005
 *  	...
 *      Raymont, Daniel Freaky Friday   2003
 *      Raymont, Daniel Alien: Resurrection     1997
 *		...
 *
 *      The output file1 (the middle result) should look like:
 *      
 *      6       Aaron, Caroline
 * 		5       Aarons, Bonnie
 *		3       Abadie, William
 *		4       Abbott, Deborah
 *		...
 *      
 *      The output file2 (the final result) should look like:
 *      
 *      38      Welker, Frank
 *		38      Tatasciore, Fred
 *		32      Jackson, Samuel L.
 *		...
 *
 *      
 *      Three argument needed:
 *      Input: "/home/azhang/workspace/hadoop-class-example/data/movie"
 *      Output 1: "/home/azhang/workspace/hadoop-class-example/data/movie_HW2part2_MR1_out1a"
 *      Output 2: "/home/azhang/workspace/hadoop-class-example/data/movie_HW2part2_MR2_out1a"
 *      
 **/


import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ucsc.hadoop.util.ConfigurationUtil;

public class Homework2Part2 extends Configured implements Tool  {

	private static final Log LOG = LogFactory.getLog(Homework2Part1.class);
	
	public static class HW2Part2Mapper1 extends Mapper<Object, Text, Text, IntWritable>
	{
		private IntWritable increment = new IntWritable(1);		
		private Text actor = new Text();
				
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException
		{
			String[] tokens = value.toString().split("\\t");
			
			if (tokens.length == 3)
			{
				// Get the actor name from the input line.
				actor.set(tokens[0]);
				// Add another count of the actor to the context by mapping value 1 to the actor.
				context.write(actor, increment);
			}
		}
	}

	public static class HW2Part2Reducer1 extends Reducer<Text, IntWritable, IntWritable, Text>
	{
		private IntWritable reducerOutput = new IntWritable();
		
		@Override
		public void reduce(Text actor, Iterable<IntWritable> values, Context context) 
				 throws IOException, InterruptedException
		{
			int sum = 0;

			// For each of the counts associated with the actor.
			for (IntWritable val : values)
			{
				// Increment the total number.
				sum = sum + val.get();
			}

			reducerOutput.set(sum);
			// Associate the actor to the total number.
			context.write(reducerOutput, actor);			
		}
	}

	// Custom writable object used to sort numbers in reverse order.
	public static class CustomWritable extends IntWritable
	{
		public int compareTo(Object obj)
		{
			int result = 0;
			int val = super.compareTo(obj);
			if (val == 0)
			{
				result = 0;
			}
			else if (val > 0)
			{
				// Inverse order so return -1.
				result = -1;
			}
			else
			{
				// Inverse order so return 1.
				result = 1;
			}
			return result;
		}
		
	}

	public static class HW2Part2Mapper2 extends Mapper<Object, Text, CustomWritable, Text>
	{
		// Use the custom IntWritable for sorting.
		private CustomWritable sumCount = new CustomWritable();		
		private Text actor = new Text();
				
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException
		{
			String[] tokens = value.toString().split("\\t");
			
			if (tokens.length == 2)
			{
				int count = Integer.parseInt(tokens[0]);
				sumCount.set(count);
				actor.set(tokens[1]);
				// Map the actor to the number of movies actor is in.
				context.write(sumCount, actor);
			}
		}
	}

	public static class HW2Part2Reducer2 extends Reducer<CustomWritable, Text, CustomWritable, Text> {
		private Text reducerOutput = new Text();
		
		@Override
		public void reduce(CustomWritable count, Iterable<Text> values, Context context) 
				 throws IOException, InterruptedException {
			// For each of the actors...
			for (Text actor : values) {
				reducerOutput.set(actor);
				// Map the actor to the number of movies.
				// Using the custom IntWritable Hadoop takes care of reverse ordering the final result.
				context.write(count, reducerOutput);				
			}
		}
	}
	
	public int run(String[] args) throws Exception
	{
		int runOutput = 0;
		Configuration conf = getConf();
		
		ConfigurationUtil.dumpConfigurations(conf, System.out);
		
		Job j1 = new Job(conf, "HW2Part2_1");
		j1.setJarByClass(Homework2Part2.class);

		j1.setMapperClass(HW2Part2Mapper1.class);
		j1.setReducerClass(HW2Part2Reducer1.class);

		j1.setMapOutputKeyClass(Text.class);
		j1.setMapOutputValueClass(IntWritable.class);
		
		j1.setOutputKeyClass(Text.class);
		j1.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(j1, new Path(args[0]));
		FileOutputFormat.setOutputPath(j1, new Path(args[1]));
		
		boolean result = j1.waitForCompletion(true);

		if (result)
		{
		    Job j2 = new Job(conf, "HW2Part2_2");
		    j2.setJarByClass(Homework2Part2.class);

		    j2.setMapperClass(HW2Part2Mapper2.class);
		    j2.setReducerClass(HW2Part2Reducer2.class);
	
		    j2.setMapOutputKeyClass(CustomWritable.class);
		    j2.setMapOutputValueClass(Text.class);
			
		    j2.setOutputKeyClass(CustomWritable.class);
		    j2.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(j2, new Path(args[1]));
			FileOutputFormat.setOutputPath(j2, new Path(args[2]));
		
	        result = j2.waitForCompletion(true);
		}
		
		if (result)
		{
			runOutput = 0;
		}
		else
		{
			runOutput = 1;
		}
		return runOutput;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.out.println(Homework2Part2.class.getName());
		int res = ToolRunner.run(new Homework2Part2(), args);
		System.exit(res);		
	}

}
