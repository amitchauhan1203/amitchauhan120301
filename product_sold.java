
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
        
public class product_sold {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
   
    //private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	  IntWritable one = new IntWritable(1);
        String line = value.toString();
        String [] part = line.split(","); 
            context.write(new Text(part[0]), one);
        System.out.println(line);
        System.out.println(part[0]);
        System.out.println(part[1]);
        System.out.println(value);
        System.out.println("***************");
        
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
        
    Job job = new Job(conf, "product_sold");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path("/home//cloudera//Desktop//cas//productdata//"));
    FileOutputFormat.setOutputPath(job, new Path("/home//cloudera//Desktop//cas//output_total"));
        
    job.waitForCompletion(true);
 }
        
}
