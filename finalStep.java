package org.apache.hadoop.ramapo;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Scanner;
import java.util.ArrayList;
import java.io.File;
import java.io.FileNotFoundException;
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
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class finalStep {
  public static class myMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
		public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String [] lineValues = value.toString().split("\t");
      IntWritable userID = new IntWritable(Integer.parseInt(lineValues[0]));
      IntWritable friendID = new IntWritable(Integer.parseInt(lineValues[1]));
      context.write(userID,friendID);
		}
  }

  public static class myReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    public void reduce (IntWritable keys, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       ArrayList<Integer> friendLikes = new ArrayList<Integer>();
       ArrayList<Integer> userLikes = new ArrayList<Integer>();
       ArrayList<Integer> userFriends = new ArrayList<Integer>();

       for (IntWritable value : values) {
         userFriends.add(value.get());
       }
       File theFile = new File ("likedByUser/part-r-00000");
       Scanner scanner = new Scanner (theFile);
       while(scanner.hasNextLine()) {
         String line = scanner.nextLine();
         String[] lineVal = line.split("\t");
         if (Integer.parseInt(lineVal[0]) == keys.get()) {
           userLikes.add(Integer.parseInt(lineVal[1]));
         }
         for(int i=0; i<userFriends.size(); i++) {
           if(userFriends.get(i) == Integer.parseInt(lineVal[0])) {
             if (!friendLikes.contains(lineVal[1])) {
             friendLikes.add(Integer.parseInt(lineVal[1]));
            }
           }
         }
        }

        friendLikes.removeAll(userLikes);
        for (int i=0; i<friendLikes.size(); i++) {
          context.write(keys, new IntWritable(friendLikes.get(i)));
        }

     }
   }
















  public static void main(String[] args) throws Exception {
  		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "finalStep");
		job.setJarByClass(finalStep.class);
		job.setMapperClass(myMapper.class);
		job.setReducerClass(myReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
