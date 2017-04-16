

	import java.io.FileNotFoundException;
	import java.io.IOException;
	import java.net.URI;
	import java.net.URISyntaxException;
	import java.util.regex.Matcher;
	import java.util.regex.Pattern;
	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.FileStatus;
	import org.apache.hadoop.fs.FileSystem;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.Mapper;
	import org.apache.hadoop.mapreduce.Reducer;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	/**
	 * Project-Group 3
	 *Date Created - -4/09/2017
	 *Description - Extracts the tweets and screen name from Json file
	 */
	/**
	 * @author hanis
	 *
	 */
	public class JsonExtractor {
		
	        /**
	         * Mapper Class - Takes input as json file and outputs screen name and text and key value pair
	         *
	         */
	        public static class JsonExtractorMapper extends Mapper<LongWritable, Text, Text, Text>{
	        Text outKey = new Text();
	        
	       /** 
	     * Mapper Method
	     */
	    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
	   String eachInputFileLine =value.toString().trim();
	   Pattern pattern1 = Pattern.compile("\\\"text.*\\\",\\\"place");
	   Matcher matcher1 = pattern1.matcher(eachInputFileLine);
	   Pattern pattern2 = Pattern.compile("\\\"lang.*\\\",\\\"favorited\\\":");
	   Matcher matcher2 = pattern2.matcher(eachInputFileLine);
	   Pattern pattern3 = Pattern.compile("\\\"screen_name.*\\\",\\\"id_str");
	   Matcher matcher3 = pattern3.matcher(eachInputFileLine);
	   if (matcher1.find() && matcher2.find() && matcher3.find() && matcher2.group(0).contains("\"en\""))
	   {
	   String [] entries = matcher1.group(0).split("\\\",");
	   String [] user_id = matcher3.group(0).split("\\\",");
	       for(String entry: entries){
	                   if(entry.contains("\"text\":"))
	                   {
	                        String [] text = entry.split(",");
	                        for(String tex : text){
	                                if(tex.contains("\"text\":"))
	                                        {
	                                        context.write(new Text(user_id[0].substring(15)),new Text(tex));

	           }
	           }
	           }
	           }
	   }

	}
	}
	        
	        /**
	         * Reducer Class - Takes the screen name and text and outputs the same 
	         *
	         */
	        public static class JsonExtractorReducer extends Reducer<Text,Text,Text,Text>{
	        	
	        	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException{
	        	for(Text value: values){
	        	        context.write(key, value);
	        	}
	        	}
	        	}

	        	/**
	        	 * Driver Method
	        	 */
	        	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
	        	        Configuration configuration = new Configuration();
	        	    FileSystem hdfs =FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration);
	        	    Job job = Job.getInstance(configuration, "Json Extractor");
	        	    job.setJarByClass(JsonExtractor.class);
	        	    job.setMapperClass(JsonExtractorMapper.class);
	        	    job.setMapOutputValueClass(Text.class);
	        	    job.setMapOutputKeyClass(Text.class);
	        	    job.setReducerClass(JsonExtractorReducer.class);
	        	    try{
	        	    Path inputPath = new Path(args[0]);
	        	    @SuppressWarnings("unused")
	        	    FileStatus fileListStatus = hdfs.getFileStatus(inputPath);
	        	    Path outputPath = new Path(args[1]);
	        	    if(!hdfs.exists(outputPath))
	        	{
	        	    FileInputFormat.addInputPath(job, new Path(args[0]));
	        	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        	    job.setOutputKeyClass(Text.class);
	        	    job.setOutputValueClass(Text.class);
	        	    System.exit(job.waitForCompletion(true)? 0 : 1);
	        	}else{
	        	    System.out.println("Output Path already exists. Please give an output Path which dosen't exits");
	        	}
	        	    }catch(FileNotFoundException e){
	        	            System.out.println("Input path is not valid");
	        	    }
	        	        }

	        	}



