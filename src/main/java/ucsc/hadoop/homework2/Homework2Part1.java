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
 *      The output file should look like:
 *          
 *      Freaky Friday   McClure, Marc (I); Raymont, Daniel; Lohan, Lindsay; Curtis, Jamie Lee; Gonzalo, Julie;
 *      ...
 *      Superman II     Donner, Richard; O'Halloran, Jack; Hollis, John (I); McClure, Marc (I);
 *      ...    
 *      
 *      Two argument:
 *      Input: "/home/azhang/workspace/hadoop-class-example/data/movie"
 *      Output: "/home/azhang/workspace/hadoop-class-example/data/movie_out_part1"
 *      
 **/

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ucsc.hadoop.util.ConfigurationUtil;

public class Homework2Part1 extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(Homework2Part1.class);
	
	public static class HW2Part1Mapper extends Mapper<Object, Text, Text, Text>
	{
		private Text movieTitle = new Text();
		private Text actor = new Text();
				
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException
		{
			
			String[] tokens = value.toString().split("\\t");
			
			if (tokens.length == 3)
			{
				// Get the movie title from the input line field.
				movieTitle.set(tokens[1]);
				// Get the actor name from the input line field.
				actor.set(tokens[0]);
				// Map actor to movie.
				context.write(movieTitle, actor);
			}
		}
	}

	public static class HW2Part1Reducer extends Reducer<Text, Text, Text, Text>
	{
		private Text reducerOutput = new Text();
		private StringBuilder actors = new StringBuilder();
		
		@Override
		public void reduce(Text movie, Iterable<Text> values, Context context) 
				 throws IOException, InterruptedException
		{
			actors.setLength(0);
			// For each of the actors in the list...
			for (Text val : values)
			{
				// Add the actor name to semi-colon separated list.
				actors.append(val.toString());
				actors.append("; ");
			}
			// Set the list of actors.
			reducerOutput.set(actors.toString());
			// To the input movie, associate the list of actors.
			context.write(movie, reducerOutput);
		}
	}

	public int run(String[] args) throws Exception
	{
		int runResult = 0;

		Configuration conf = getConf();
		
		ConfigurationUtil.dumpConfigurations(conf, System.out);
		
		Job job = new Job(conf, "Homework2Part1");
		job.setJarByClass(Homework2Part1.class);
		job.setMapperClass(HW2Part1Mapper.class);
		job.setReducerClass(HW2Part1Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);

		if (result)
		{
			runResult = 0;
		}
		else
		{
			runResult = 1;
		}
		return runResult;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception
	{
		System.out.println(Homework2Part1.class.getName());
		
		int exitCode = ToolRunner.run(new Homework2Part1(), args);
		
		System.exit(exitCode);		
	}
	
}