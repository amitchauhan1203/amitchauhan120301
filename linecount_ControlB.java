import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;
        
public class linecount_ControlB {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    //private Text word = new Text();
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	  String line = value.toString();
          StringTokenizer tokenizer = new StringTokenizer(line);
          while (tokenizer.hasMoreTokens()) {
              word.set(tokenizer.nextToken());
              context.write(word, one);
          }
        
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
 }
 
 private static void init() {
		//DOMConfigurator.configure("myapp-log4j.xml");
		// OR for property file, should use any one of these
		PropertyConfigurator.configure("myapp-log4j.properties");
	}
        
 public static void main(String[] args) throws Exception {
	
	 //init ();
    Configuration conf = new Configuration();
    
    conf.set("mapred.min.split.size", "99999999");
    conf.set("mapred.max.split.size", "99999999");
   
        
    @SuppressWarnings("deprecation")
	Job job = new Job(conf, "linecount");
    
  
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    //job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    
    job.setNumReduceTasks(0);
    
    
    //job.set
    
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    
    FileInputFormat.setInputDirRecursive(job, true);
    
    FileInputFormat.addInputPath(job, new Path("hdfs://quickstart.cloudera:8020/user/file/"));
    
    FileOutputFormat.setOutputPath(job, new Path("/home//cloudera//Desktop//cas//output"));
        
    job.waitForCompletion(true);
 }
        
}