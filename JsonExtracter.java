/*
 * Author -Project-Group 3
 * Date Created - -3/15/2017
 * Description- The Program takes three arguments -Input Path ,Output Path, number of files needs to be parsed and generates csv file which contains user and tweets
*/

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

public class JsonExtracter {
        public static void main(String args[]) throws IOException, URISyntaxException{ 
		int inputFileCount = 0;
			try{
			inputFileCount = Integer.parseInt(args[2]);
		}catch(NumberFormatException e){ System.out.println("Please enter a valid count");System.exit(1);
		}	     
			/*creating a file system object*/
                Configuration conf = new Configuration();
    	        FileSystem hdfs =FileSystem.get(new URI("hdfs://hadoop1:9000"), conf);
				/*check if the input path exits or not*/
    	        try{
    	        	Path inputPath = new Path(args[0]);
    	        	FileStatus[] fileListStatus = hdfs.listStatus(inputPath);
    	        	Path[] filePaths = FileUtil.stat2Paths(fileListStatus);
    	        	Path outputFolder = new Path(args[1]);
					/*check if the output path exists and create the folder if not exists*/
    	        	if(!hdfs.exists(outputFolder)){
    	        		hdfs.mkdirs(outputFolder);
    	        	}
					/*read the file one by one*/
    	        	for(int i=0;i<inputFileCount;i++){
						/*create outputstream for each input file*/
			Path outputFile = new Path(args[1]+"//tweetdata_"+i+".csv");
                        OutputStream outputStream = hdfs.create( outputFile,
                                            new Progressable() {
                                                public void progress() {
                                                } });
												
                        BufferedWriter bufferedWriter = new BufferedWriter( new OutputStreamWriter( outputStream, "UTF-8" ) );
						/*create buffered reader object for the input files*/
    	        	BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(filePaths[i])));
    	        	String line = bufferedReader.readLine();
					/*read the file contents line by line till end of the line*/
                 while(line != null){
                        String eachInputFileLine = line.trim();
						/*create pattern objects for different regular expression and extract tweet and its user*/
                        Pattern pattern1 = Pattern.compile("\\\"text.*\\\",\\\"place");
                        Matcher matcher1 = pattern1.matcher(eachInputFileLine);
                        Pattern pattern2 = Pattern.compile("\\\"lang.*\\\",\\\"favorited\\\":");
                        Matcher matcher2 = pattern2.matcher(eachInputFileLine);
                        Pattern pattern3 = Pattern.compile("\\\"screen_name.*\\\",\\\"id_str");
                        Matcher matcher3 = pattern3.matcher(eachInputFileLine);
						/*print the contents of a file if matched with above regular expressions*/
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
                                        			bufferedWriter.write(user_id[0].substring(15)+","+tex);
								bufferedWriter.newLine();
                                        			
                                }
                                }
                                }
                                }
                        }
			 line = bufferedReader.readLine();
                }
				/*close the buffered reader and buffered writer object*/
			 bufferedWriter.close();
			 bufferedReader.close();
    	        	}
        	        hdfs.close();
                }catch(FileNotFoundException e){
                        System.out.println("File not found");
                }
        }

}

