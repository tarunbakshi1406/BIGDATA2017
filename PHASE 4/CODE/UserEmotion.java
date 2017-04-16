


import java.io.IOException;
import java.net.URISyntaxException;
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
/**
 * Project-Group 3
 * Date Created - -3/21/2017
 * Description- The Program takes three arguments -Input Path ,Output Path and outputs user emotion pair
 */
public class UserEmotion
{
/**
 * Mapper Class - Takes input as output from Json Extractor and outputs the user and emotion*/
        public static class UserEmotionMapper extends Mapper<LongWritable, Text, Text, Text>
        {
                Text outKey = new Text();
            Text outValue = new Text();
                public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException
                {
                    String[] columns=value.toString().split("   ");
                    String userName = columns[0];
                    String tweet = columns[1];
                    String[] tweet1 = tweet.split("\\s+");
                        outKey.set(userName);
                        String[] keyword = {"delight","great","pleasure","joyfulness","jubilation","triumph","exultation","rejoicing",
                                        "happiness","gladness","glee","exhilaration","exuberance","elation","euphoria","bliss","ecstasy","rapture",
                                        "enjoyment","felicity","jouissance","unhappiness","sorrow","dejection","depression","misery","despondency",
                                        "despair","desolation","wretchedness","gloom","gloominess","dolefulness","melancholy","mournfulness","woe",
                                        "heartache","grief","shock","bolt from the blue","bombshell","revelation","rude awakening","eye-opener",
                                        "wake-up call","astonishment","amazement","wonder","incredulity","bewilderment","stupefaction","disbelief",
                                        "astonish","amaze","startle","astound","stun","stagger","shock","dumbfound","stupefy","daze","astonished",
                                        "amazed","astounded","startled","stunned","staggered","nonplussed","shocked","taken aback","stupefied",
                                        "dumbfounded","dumbstruck","speechless","thunderstruck","confounded","shaken up","unexpected","unforeseen",
                                        "unpredictable","astonishing","amazing","startling","astounding","staggering","incredible","extraordinary",
                                        "breathtaking","remarkable","rage","vexation","exasperation","displeasure","crossness","irritation",
                                        "irritability","indignation","pique; annoyance","fury","wrath","ire","outrage","irascibility","ill temper",
                                        "aggravation","infuriate","irritate","exasperate","irk","vex","peeve","madden","enrage","incense","annoy","terror",
                                        "fright","fearfulness","horror","alarm","panic","agitation","trepidation","dread","consternation",
                                        "dismay","distress; anxiety","worry","angst","unease","uneasiness","apprehension","apprehensiveness","nervousness",
                                        "nerves","perturbation","foreboding","phobia","aversion","antipathy","bugbear","nightmare","horror","terror",
                                        "anxiety","neurosis","revulsion","repugnance","aversion","distaste","nausea","abhorrence","loathing",
                                        "detestation","odium","horror; contempt","outrage","revolt","repel","repulse","sicken","nauseatef"};
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

/**
 * Reducer Class - outputs the user and emotion as key value pair*/
    public static class UserEmotionReducer extends Reducer<Text,Text,Text,IntWritable>
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
/**Driver class*/

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException
    {

                    Configuration configuration = new Configuration();
                    configuration.set("fs.defaultFS", "hdfs://hadoop1:9000/");
            Job job = Job.getInstance(configuration, "Emotion-User Pair");
            job.setJarByClass(UserEmotion.class);
            job.setMapperClass(UserEmotionMapper.class);
            job.setMapOutputValueClass(Text.class);
            job.setMapOutputKeyClass(Text.class);
            job.setReducerClass(UserEmotionReducer.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            System.exit(job.waitForCompletion(true)? 0 : 1);

    }

}

