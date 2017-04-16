

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import java.nio.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.fs.FileSystem;

/**
 * Project-Group 3
 *Date Created - 4/11/2017
 *Description - Takes input as output from User Emotion program and outputs the probability distribution of each emotion 
 */
public class DominantEmotion
{

        /**
         * Mapper - Takes the input and map the synonym of emotions to one of 6 Ekmans emotions
         *
         */
        public static class DominantEmotionMapper extends Mapper<LongWritable, Text, Text, Text>
        {
                public void map(LongWritable Key, Text value, Context con) throws IOException,InterruptedException
                {       /* for splitting of words and then assigning it to certain variables */
                        String[] words =value.toString().split("        ");
                        String Name = words[0];
                        String Emotion = words[1];
                        if(Emotion.contains("happy")||Emotion.contains("delight")||Emotion.contains("great")||Emotion.contains("pleasure")||Emotion.contains("joy") ||Emotion.contains("jubilation")||Emotion.contains("triumph")||Emotion.contains("exultation")||Emotion.contains("rejoicing")||Emotion.contains("happiness")||Emotion.contains("gladness")||Emotion.contains("glee")||Emotion.contains("exhilaration")||Emotion.contains("exuberance")||Emotion.contains("elation")||Emotion.contains("euphoria")||Emotion.contains("bliss")||Emotion.contains("ecstasy")||Emotion.contains("rapture")||Emotion.contains("enjoyment")||Emotion.contains("felicity")||Emotion.contains("jouissance"))
                        {
                                Emotion = "happy";
                }else if(Emotion.contains("sad")||Emotion.contains("unhappiness")||Emotion.contains("sorrow")||Emotion.contains("dejection")||Emotion.contains("depression")||Emotion.contains("misery")||Emotion.contains("despondency")||Emotion.contains("despair")||Emotion.contains("desolation")||Emotion.contains("wretchedness")||Emotion.contains("gloom")||Emotion.contains("gloominess")||Emotion.contains("dolefulness")||Emotion.contains("melancholy")||Emotion.contains("mournfulness")||Emotion.contains("woe")||Emotion.contains("heartache")||Emotion.contains("grief"))
                        {
                                Emotion = "sad";
                        }else if( Emotion.contains("surprise")|| Emotion.contains("remarkable")||Emotion.contains("shock")||Emotion.contains("bolt from the blue")||Emotion.contains("bombshell")||Emotion.contains("revelation")||Emotion.contains("rude awakening")||Emotion.contains("eye-opener")||Emotion.contains("wake-up call")||Emotion.contains("astonishment")||Emotion.contains("amazing")||Emotion.contains("wonder")||Emotion.contains("incredulity")||Emotion.contains("bewilderment")||Emotion.contains("stupefaction")||Emotion.contains("disbelief")||Emotion.contains("astonish")||Emotion.contains("amaze")||Emotion.contains("startle")||Emotion.contains("astound")||Emotion.contains("stun")||Emotion.contains("stagger")||Emotion.contains("shock")||Emotion.contains("dumbfound")||Emotion.contains("stupefy")||Emotion.contains("daze")||Emotion.contains("astonished")||Emotion.contains("amazed")||Emotion.contains("astounded")||Emotion.contains("startled")||Emotion.contains("stunned")||Emotion.contains("staggered")||Emotion.contains("nonplussed")||Emotion.contains("shocked")||Emotion.contains("taken aback")||Emotion.contains("stupefied")||Emotion.contains("dumbfounded")||Emotion.contains("dumbstruck")||Emotion.contains("speechless")||Emotion.contains("thunderstruck")||Emotion.contains("confounded")||Emotion.contains("shaken up")||Emotion.contains("unexpected")||Emotion.contains("unforeseen")||Emotion.contains("unpredictable"))
                        {
                        Emotion="surprise";
                        }else if(Emotion.contains("anger")||Emotion.contains("rage")||Emotion.contains("vexation")||Emotion.contains("exasperation")||Emotion.contains("displeasure")||Emotion.contains("crossness")||Emotion.contains("irritation")||Emotion.contains("irritability")||Emotion.contains("indignation")||Emotion.contains("pique")||Emotion.contains("annoyance")||Emotion.contains("fury")||Emotion.contains("wrath")||Emotion.contains("ire")||Emotion.contains("outrage")||Emotion.contains("irascibility")||Emotion.contains("ill temper")||Emotion.contains("aggravation")||Emotion.contains("infuriate")||Emotion.contains("irritate")||Emotion.contains("exasperate")||Emotion.contains("irk")||Emotion.contains("vex")||Emotion.contains("peeve")||Emotion.contains("madden")||Emotion.contains("enrage")||Emotion.contains("incense")||Emotion.contains("annoy"))
                        {
                        Emotion="anger";
                        }else if(Emotion.contains("fear")||Emotion.contains("terror")||Emotion.contains("fright")||Emotion.contains("fearfulness")||Emotion.contains("horror")||Emotion.contains("alarm")||Emotion.contains("panic")||Emotion.contains("agitation")||Emotion.contains("trepidation")||Emotion.contains("dread")||Emotion.contains("consternation")||Emotion.contains("dismay")||Emotion.contains("distress")||Emotion.contains("worry")||Emotion.contains("angst")||Emotion.contains("unease")||Emotion.contains("uneasiness")||Emotion.contains("apprehension")||Emotion.contains("apprehensiveness")||Emotion.contains("nervousness")||Emotion.contains("nerves")||Emotion.contains("perturbation")||Emotion.contains("foreboding")||Emotion.contains("phobia")||Emotion.contains("aversion")||Emotion.contains("antipathy")||Emotion.contains("bugbear")||Emotion.contains("nightmare")||Emotion.contains("horror")||Emotion.contains("terror")||Emotion.contains("anxiety")||Emotion.contains("neurosis"))
                        {
                                Emotion="fear";
                        }else if(Emotion.contains("disgust")||Emotion.contains("revulsion")||Emotion.contains("repugnance")||Emotion.contains("aversion")||Emotion.contains("distaste")||Emotion.contains("nausea")||Emotion.contains("abhorrence")||Emotion.contains("loathing")||Emotion.contains("detestation")||Emotion.contains("odium")||Emotion.contains("horror")||Emotion.contains("contempt")||Emotion.contains("outrage")||Emotion.contains("revolt")||Emotion.contains("repel")||Emotion.contains("repulse")||Emotion.contains("sicken")||Emotion.contains("nauseate"))
                                {
                                        Emotion="disgust";
                        }
                        else{
                                Emotion="unknown";
                        }
                        con.write(new Text(Name),new Text(Emotion));
        }
}
        
        /**
         * Reducer - Calculate the probability distribution of emotions
         *
         */
        public static class DominantEmotionReducer extends Reducer<Text,Text,Text,Text>
                {
                public void reduce(Text Name, Iterable<Text> Emotion,Context con) throws IOException, InterruptedException
                {
                        int happy1=0;
                           int sad1=0;
                           int surprise1=0;
                           int disgust1=0;
                           int fear1=0;
                           int anger1=0;
                                int count=0;
                                float happy = 0.0f;
                                float sad = 0.0f;
                                float surprise = 0.0f;
                                float anger = 0.0f;
                                float fear = 0.0f;
                                float disgust = 0.0f;
                                ArrayList<String> emotions = new ArrayList<String>();
                                for(Text emotion: Emotion){
                                count++;
                        emotions.add(emotion.toString());
                                }
                           for(String emotion : emotions){
                        	   System.out.println("hi");
                               if(emotion.contains("happy"))
                            {
                                    ++happy1;
                                    happy = (float)happy1/count;
                            }else if(emotion.contains("sad"))
                            {
                                    ++sad1;
                                    sad = (float)sad1/count;
                            }else if(emotion.contains("surprise"))
                            {
                            ++surprise1;
                            surprise = (float)surprise1/count;
                            System.out.println(Name.toString()+" "+surprise1+" "+count);
                            }else if(emotion.contains("anger"))
                            {
                            ++anger1;
                            anger = (float)anger1/count;
                            System.out.println(Name.toString()+" "+anger1+" "+count);
                            }else if(emotion.contains( "fear"))
                            {
                                    ++fear1;
                            fear = (float)fear1/count;
                            }else if(emotion.contains("disgust"))
                                    {
                                    ++disgust1;
                            disgust=disgust1/count;
                            }
                            else if(emotion.contains("unknown")){
                            }
                    }
                            con.write(new Text(Name),new Text(","+happy+","+sad+","+surprise+","+anger+","+fear+","+disgust));
                    }
            }
            /**
             *Driver Method
             */
            public static void main(String args[]) throws Exception
            {
                    Configuration c = new Configuration();
                    Path p1 = new Path(args[0]);
                    Path p2 = new Path(args[1]);
                    Job j = Job.getInstance(c,"Dominant Emotion");
                    j.setJarByClass(DominantEmotion.class);
                    j.setMapperClass(DominantEmotionMapper.class);
                    j.setReducerClass(DominantEmotionReducer.class);
                    j.setOutputKeyClass(Text.class);
                    j.setOutputValueClass(Text.class);
                    FileInputFormat.addInputPath(j,p1);
                    FileOutputFormat.setOutputPath(j,p2);
                    System.exit(j.waitForCompletion(true) ? 0 : 1);
            }
     }