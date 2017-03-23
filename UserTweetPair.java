/*
 * Author -Project-Group 3
 * Date Created - -3/19/2017
 * Description- The Program takes three arguments -Input Path ,Output Path and outputs user tweet pair
*/
	
	import java.io.FileNotFoundException;
	import java.io.IOException;
	import java.net.URI;
	import java.net.URISyntaxException;
	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.FileStatus;
	import org.apache.hadoop.fs.FileSystem;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.Mapper;
	import org.apache.hadoop.mapreduce.Reducer;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

	public class UserTweetPair {
		/*Mapper class for getting the key value pair as username and tweet*/
		public static class NoOfTweetsMapper extends Mapper<LongWritable, Text, Text, Text>
		{
			 Text outKey = new Text();
	         Text outValue = new Text();

			public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
	            String[] columns=value.toString().split(",/"text/"/:");
	            String userName = columns[0];
	            String tweet = columns[1];	   
	            	outValue.set(tweet);
	  	            outKey.set(userName);
	  	            context.write(outKey, outValue);
	                      
	    }
		}	
/*Reducer class for getting the key value pair as username and tweet*/		
		public static class NoOfTweetsReducer extends Reducer<Text,Text,Text,Text> {
	 
	
	    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
	   
	      for (Text val : values) {
	       context.write(key, val);
	      }	      
	    }
	  }
	  /*Driver class to run the User Tweet Pair job*/

		public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException 
		{
			
			Configuration configuration = new Configuration();
			configuration.set("fs.defaultFS", "hdfs://hadoop1:9000/");
	        Job job = Job.getInstance(configuration, "User Tweet Pair");
	        job.setJarByClass(UserTweetPair.class);
	        job.setMapperClass(NoOfTweetsMapper.class);
	        job.setMapOutputValueClass(Text.class);
	        job.setMapOutputKeyClass(Text.class);
	        job.setReducerClass(NoOfTweetsReducer.class);
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        System.exit(job.waitForCompletion(true)? 0 : 1);
	       
		}

	}



