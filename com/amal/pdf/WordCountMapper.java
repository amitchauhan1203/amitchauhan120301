package com.amal.pdf;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
 
        
    public void map(LongWritable key, Text value,Context context) throws java.io.IOException ,InterruptedException
    {
        String lines = value.toString();
        String []lineArr = lines.split("\n");
        int lcount = lineArr.length;
        context.write(new Text(new Integer(lcount).toString()),new IntWritable(1));
     }
        
    }
  
