package com.netease.bigdate;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class MyMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line," ");
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        context.write(word,one);
      }
    }
  }

  public static class MyReduce extends Reducer<Text,IntWritable,Text,IntWritable> {

    private final IntWritable res = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

      int sum = 0;

      for (IntWritable num : values) {
        sum += num.get();
      }
      res.set(sum);
      context.write(key,res);
    }
  }

  public static void main(String[] args)
    throws IOException, ClassNotFoundException, InterruptedException {
    Configuration configuration = new Configuration();
    Job job = Job.getInstance(configuration);

    // 设置jar包及作业名称
    job.setJarByClass(WordCount.class);
    job.setJobName("wordcount");

    // 设置Mapper和Reducer实现
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReduce.class);

    // 设置输出格式
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // 输入输出路径
    FileInputFormat.addInputPath(job,new Path(args[0]));
    FileOutputFormat.setOutputPath(job,new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
