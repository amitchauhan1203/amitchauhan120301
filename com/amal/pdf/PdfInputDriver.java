package com.amal.pdf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PdfInputDriver {

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		args = parser.getRemainingArgs();
		Job job = new Job(conf, "Pdfwordcount");
		job.setJarByClass(PdfInputDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(PdfInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		  
		FileInputFormat.addInputPath(job, new Path("/home//cloudera//Desktop//cas//inputline"));
	    FileOutputFormat.setOutputPath(job, new Path("/home//cloudera//Desktop//cas//output"));
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		System.out.println(job.waitForCompletion(true));
	}
	}

