/*
 * Name -Project-Group 3
 * 
 * Date Created - -3/21/2017
 * Description- The Program takes three arguments -Input Path ,Output Path and outputs user emotion pair
*/

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserEmotion 
{	
	public static class NoOfTweetsMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		Text outKey = new Text();
	    Text outValue = new Text();
		public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException 
		{
	            String[] columns=value.toString().split(",");
	            String userName = columns[0];
	            String tweet = columns[1];	  
	            String[] tweet1 = tweet.split("\\s+");
	  	        outKey.set(userName);
	  	        String[] keyword = {"joy","Joy","politics","enjoy","sadness","anger","surprize","anxiety","good","amazing"};
	  	          for(String s:tweet1)  	        	  
	  	          {
	  	        	  for(String k:keyword)
	  	        	  {
	  	        		if(String.valueOf(s).contains(String.valueOf(k)))
	  	        		{
	  	        			context.write(outKey,new Text(s));
	  	        		}
	  	        	  }
	  	        	  
	  	        }  	                                           
	  	          
		}
	}
	public static class MapJoinReducer extends Reducer<Text,Text,Text,IntWritable> 
	{
		    private IntWritable count = new IntWritable();
		    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		      int sum = 0;
		      for (IntWritable val : values) {
		        sum += val.get();
		      }
		      count.set(sum);
		      context.write(key, count);
		    }
		  }
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException 
	{
			
			Configuration configuration = new Configuration();
			configuration.set("fs.defaultFS", "hdfs://hadoop1:9000/");
	        Job job = Job.getInstance(configuration, "Emotion-User Pair");
	        job.setJarByClass(UserEmotion.class);
	        job.setMapperClass(NoOfTweetsMapper.class);
	        job.setMapOutputValueClass(Text.class);
	        job.setMapOutputKeyClass(Text.class);
	        job.setReducerClass(MapJoinReducer.class);
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        System.exit(job.waitForCompletion(true)? 0 : 1);
	       
	}

}



