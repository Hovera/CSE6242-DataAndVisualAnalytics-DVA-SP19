package edu.gatech.cse6242;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Q1 {

	public static class TokenizerMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
		public void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException {

			String data = value.toString();
			String[] split_data = data.split("\t");
			IntWritable receiver = new IntWritable(Integer.parseInt(split_data[1]));
			IntWritable weight = new IntWritable(Integer.parseInt(split_data[2]));
			context.write(receiver, weight);
		}		
	}

	// from apache
	public static class IntSumReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
				
		    	int sum = 0;
		    	for (IntWritable val : values) {
		    		sum += val.get();
		    	}
		      	result.set(sum);
		      	context.write(key, result);
		    }
		  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q1");
    job.setJarByClass(Q1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

