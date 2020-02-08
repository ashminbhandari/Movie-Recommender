package org.apache.hadoop.ramapo;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Proj1 {
  public static class myMapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] lineValue = value.toString().split(",");
			context.write(new Text(lineValue[0]), new Text(lineValue[1] + "," + lineValue[2]));
		}
  }

  public static class myReducer extends Reducer<Text,Text,Text,Text> {
    		public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				for (Text value: values) {
					String[] strValue = value.toString().split(",");
					if (Float.parseFloat(strValue[1]) >= 3) {
						context.write(key, new Text(strValue[0])); 
					}
				} 
			}		
		}
 

  public static void main(String[] args) throws Exception {
  		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Proj1");
		job.setJarByClass(Proj1.class);
		job.setMapperClass(myMapper.class);
		job.setReducerClass(myReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

