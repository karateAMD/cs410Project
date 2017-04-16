package com.cs410.summarystats;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by angelading on 4/15/17.
 */
public class PrettifyOutputJob extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        return 0; // TODO
    }

    private class PrettifyOutputMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // TODO
        }
    }
}
