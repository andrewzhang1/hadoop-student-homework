package ucsc.hadoop.homework2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import ucsc.hadoop.homework2.Homework2Part2;

public class MovieCountActorTest {
	private static final Log LOG = LogFactory.getLog(MovieCountActorTest.class);
	
	@Test
	public void mapperTest() throws Exception {
		LOG.info("..... inside mapper test");
		
		MapDriver<Object, Text, Text, IntWritable> mapDriver = new MapDriver<Object, Text, Text, IntWritable>();
		
		mapDriver.withMapper(new Homework2Part2.HW2Part2Mapper1())
		.withInput(new IntWritable(10), new Text("McClure, Marc (I)	Superman	1978"))
		.withOutput(new Text("McClure, Marc (I)"), new IntWritable(1))
		.runTest();
			
		System.out.println("expected output:" + mapDriver.getExpectedOutputs());
	}
	
	@Test
	public void reducerTest() throws Exception {
		ReduceDriver<Text, IntWritable, IntWritable, Text> reduceDriver =
					new ReduceDriver<Text, IntWritable, IntWritable, Text>();
		
		List<IntWritable> valueList = new ArrayList<IntWritable>();
		valueList.addAll(Arrays.asList(new IntWritable(2), new IntWritable(3), new IntWritable(10)));
		
		Text actor = new Text("McClure, Marc (I)");
		reduceDriver.withReducer(new Homework2Part2.HW2Part2Reducer1())
		.withInput(actor, valueList)
		.withOutput(new IntWritable(7), actor)
		.runTest();
		
		System.out.println("expected output:" + reduceDriver.getExpectedOutputs());
	}
}
