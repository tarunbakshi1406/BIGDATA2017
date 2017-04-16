
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

/**
 * Project-Group 3
 *Date Created - -4/09/2017
 *Description - Takes the output of JsonExtractor and gives the output screen name and count
 */
public class NoOfTweets {
	
        /**
         * Splits the input and counts each tweet as 1. key value would be screen name and 1
         *
         */
        public static class NoOfTweetsMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
                 Text outKey = new Text();
                private final static IntWritable one = new IntWritable(1);
                public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns=value.toString().split("\t");
            String userName = columns[0];
            String tweet = columns[1];
                outKey.set(userName);
            context.write(outKey,one);
    }
        }
                 /**
                 * Counts the number of tweets per user
                 *
                 */
                public static class NoOfTweetsReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
                         Text outKey = new Text();
             public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
                 int count = 0;
                        for(IntWritable value: values){
                         count = count+value.get();
                 }
                 IntWritable outValue = new IntWritable(count);
                 context.write(key,outValue);
             }

     }
        /**
         *Driver Method
         */
        public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem hdfs =FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration);
        Job job = Job.getInstance(configuration, "No of Tweets");
        job.setJarByClass(NoOfTweets.class);
        job.setMapperClass(NoOfTweetsMapper.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(NoOfTweetsReducer.class);
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
            job.setOutputValueClass(IntWritable.class);
            System.exit(job.waitForCompletion(true)? 0 : 1);
    		}else{
            System.out.println("Output Path already exists. Please give an output Path which dosen't exits");
    	}
            }catch(FileNotFoundException e){
                    System.out.println("Input path is not valid");
            }
            }

    }
