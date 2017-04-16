

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.TreeMap;
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
 * Date Created - -4/06/2017
 * Description- The Program takes three arguments -Input Path ,Output Path and outputs Dominant Emotion of each user normalized to 1 and users tweet count.
 * 
*/
public class MeanProportionScore {
	
    /**
     * Mapper Class
     *
     */
    public static class ProportionScoreMapper extends Mapper<LongWritable, Text, Text, Text>{
    	
             /** map method to take input probability distribution and give username and dominant emotion as key value pair
             * 
             */
            public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
            	
                   Text outKey = new Text();
                   String[] emotions = {"joy","sadness","surprise","anger","fear","disguist"};
           String[] columns=value.toString().split(",");
           String userName = columns[0];

           //String [] tempArray = columns[2].split("\\|");
           double [] probabilityScores = new double[6];
           for(int i=1;i<6;i++){
                   probabilityScores[i] = Double.parseDouble(columns[i]);
           }
           double max = probabilityScores[0];
           int maxIndex = 0;
           for(int i=0; i<probabilityScores.length;i++){
                   if(probabilityScores[i]>max){
                           max = probabilityScores[i];
                           maxIndex = i;
                   }
           }
           outKey.set(userName);
           if(maxIndex == 0){
                   context.write(outKey,new Text(max+"++"+emotions[maxIndex]));
           }else if(maxIndex == 1){
                   context.write(outKey,new Text(max+"++"+emotions[maxIndex]));
           }else if(maxIndex == 2){
                   context.write(outKey,new Text(max+"++"+emotions[maxIndex]));
           }else if(maxIndex == 3){
                   context.write(outKey,new Text(max+"++"+emotions[maxIndex]));
           }else if(maxIndex == 4){
                   context.write(outKey,new Text(max+"++"+emotions[maxIndex]));
           }else if(maxIndex == 5){
                   context.write(outKey,new Text(max+"++"+emotions[maxIndex]));
           }
        }
        }
    
            /**
             * Reducer Class
             *
             */
            public static class ProportionScoreReducer extends Reducer<Text,Text,Text,Text>{

                Text outKey = new Text();

                final Map<Text,Integer> emotionTweetCount = new TreeMap<Text,Integer>();
        /** 
         * Reducer takes the key,value as username and dominant emotion and outputs the user name, dominant emotion tweets and its count
         */
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException{
                        int count = 0;
                        emotionTweetCount.put(new Text("joy"),0);
                        emotionTweetCount.put(new Text("sadness"),0);
                        emotionTweetCount.put(new Text("surprise"),0);
                        emotionTweetCount.put(new Text("anger"),0);
                        emotionTweetCount.put(new Text("fear"),0);
                        emotionTweetCount.put(new Text("disgust"),0);
        for(Text value: values){
                count++;
                String [] tweetEmotions = value.toString().split("\\+\\+");
                        Text emotion = new Text(tweetEmotions[1]);
                                        if(emotionTweetCount.containsKey(emotion)){
                                                        emotionTweetCount.put(emotion, emotionTweetCount.get(emotion)+1);
                                                                        }

        }
        Text outValue = new Text(","+emotionTweetCount.get(new Text("joy")).toString()+","+emotionTweetCount.get(new Text("sadness")).toString()+","+emotionTweetCount.get(new Text("surprise")).toString()+","+emotionTweetCount.get(new Text("anger")).toString()+","+emotionTweetCount.get(new Text("fear")).toString()+","+emotionTweetCount.get(new Text("disgust")).toString()+","+count);
        context.write(key,outValue);
        }
        }
        /**
         * Driver method
         */
        public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
                Configuration configuration = new Configuration();
        FileSystem hdfs =FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration);
        Job job = Job.getInstance(configuration, "No of Tweets");
        job.setJarByClass(MeanProportionScore.class);
        job.setMapperClass(ProportionScoreMapper.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(ProportionScoreReducer.class);
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


