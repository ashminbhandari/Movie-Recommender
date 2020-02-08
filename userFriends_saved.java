package org.apache.hadoop.ramapo;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import java.util.TreeMap;
import java.util.Vector;
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

public class userFriends_saved {
  public static class myMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
		public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] lineValue = value.toString().split("\t");
      IntWritable movieID = new IntWritable(Integer.parseInt(lineValue[1]));
      IntWritable userID = new IntWritable(Integer.parseInt(lineValue[0]));
			context.write(movieID, userID);
		}
  }

  public static class myReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
		    TreeMap<Integer, List<Integer>> tmap = new TreeMap<Integer, List<Integer>>();
    		public void reduce (IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
				List <Integer> val = new ArrayList<Integer>();
				for (IntWritable value : values) {
					val.add(value.get());
				}

        int i = 0;
        List<Integer> valCpy = new ArrayList<Integer>(val);
        while (i < val.size()) {
          Integer akey = val.get(i);
          List<Integer> current = tmap.get(akey);
          if (current == null) {
            tmap.put(akey,valCpy);
            valCpy = new ArrayList<Integer>(val);
          }
          else {
            valCpy.removeAll(current);
            current.addAll(valCpy);
            tmap.put(akey,current);
            valCpy = new ArrayList<Integer>(val);
          }
          i++;
      }
    }




      protected void cleanup (Context context) throws IOException, InterruptedException {
          for (Map.Entry<Integer, List<Integer>> entry : tmap.entrySet()) {
                Integer theKey = entry.getKey();
                List<Integer> current = new ArrayList<Integer>(entry.getValue());
                int k = 0;
                while (k < current.size()) {
                  Integer theValue = current.get(k);
                  //if(tmap.get(theValue).contains(theKey)) {
                    //tmap.get(theValue).remove(theKey);
                  //}
                  if(theKey!=theValue) {
                    context.write(new IntWritable(theKey), new IntWritable(theValue));
                  }
                  k++;
                }
              }

          }
      }









  public static void main(String[] args) throws Exception {
  		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "userFriends");
		job.setJarByClass(userFriends_saved.class);
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
