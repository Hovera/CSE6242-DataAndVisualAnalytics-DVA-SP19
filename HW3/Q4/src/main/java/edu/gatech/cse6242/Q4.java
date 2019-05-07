package edu.gatech.cse6242;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Q4 {

	static class firstMapClass extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{
				String data = value.toString();
				if(data.length()>1){
					String[] splitted_data = data.split("\t");
					IntWritable sender = new IntWritable(Integer.parseInt(splitted_data[0]));
					IntWritable receiver = new IntWritable(Integer.parseInt(splitted_data[1]));
					IntWritable out_count = new IntWritable(1);
					IntWritable in_count = new IntWritable(-1);
					context.write(sender, out_count);
					context.write(receiver, in_count);
				}
			}
	}

	static class secondMapClass extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{
				String data = value.toString();
				if(data.length()>1){
					String[] splitted_data = data.split("\t");
					IntWritable diff = new IntWritable(Integer.parseInt(splitted_data[1]));
					IntWritable count = new IntWritable(1);
					context.write(diff, count);
				}
			}
	}

	static class totalReduceClass extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException{
				int sum = 0;
				for (IntWritable val:values){
					sum += val.get();
				}
				context.write(key, new IntWritable(sum));
			}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();
		String inter_step = args[1]+"-inter_step";
		Job job1 = Job.getInstance(conf1, "Q41");
		job1.setJarByClass(Q4.class);
		job1.setMapperClass(firstMapClass.class);
		job1.setReducerClass(totalReduceClass.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(inter_step));
		job1.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Q42");
		job2.setJarByClass(Q4.class);
		job2.setMapperClass(secondMapClass.class);
		job2.setReducerClass(totalReduceClass.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(inter_step));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);	
	}
}
