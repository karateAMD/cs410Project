package com.cs410.summarystats;

/**
 * Created by angelading on 4/15/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by angelading on 3/4/17.
 */
public class SummaryStatsDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new SummaryStatsDriver(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String outputRoot = args[1];

        // intermediate output directories
        String statsCalculationOutputDir = "/intermediate_results/stats_calculations/";
        String finalOutputDir = outputRoot + "/results/";

        conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.95");

        // remove output dirs
        Runtime.getRuntime().exec("hadoop fs -rm -r /intermediate_results/").waitFor();

        // Job 0: MakeDocuments makes documents by combining all reviews for a product's good reviews
        // into one document and combining a product's bad reviews into a different document.
        // It does this for all products
        Job calculateStatsJob = Job.getInstance(conf);
        calculateStatsJob.setJobName("Calculate Summary Statistics");
        calculateStatsJob.setNumReduceTasks(30);

        calculateStatsJob.setJarByClass(SummaryStatsDriver.class);
        calculateStatsJob.setMapperClass(SummaryStatsMapper.class);
        calculateStatsJob.setReducerClass(SummaryStatsReducer.class);
        calculateStatsJob.setCombinerClass(SummaryStatsReducer.class);
        calculateStatsJob.setOutputKeyClass(Text.class);
        calculateStatsJob.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(calculateStatsJob, new Path(args[0]));
        TextOutputFormat.setOutputPath(calculateStatsJob, new Path(statsCalculationOutputDir));
        TextInputFormat.setInputDirRecursive(calculateStatsJob, true);
        TextOutputFormat.setCompressOutput(calculateStatsJob, true);
        TextOutputFormat.setOutputCompressorClass(calculateStatsJob, GzipCodec.class);
        LazyOutputFormat.setOutputFormatClass(calculateStatsJob, TextOutputFormat.class);

        boolean ret = calculateStatsJob.waitForCompletion(true);
        if (!ret) return -1;

        Job prettifyOutputJob = Job.getInstance(conf);
        prettifyOutputJob.setJobName("Prettify Output");
        prettifyOutputJob.setNumReduceTasks(0);

        prettifyOutputJob.setJarByClass(SummaryStatsDriver.class);
        prettifyOutputJob.setMapperClass(PrettifyOutputMapper.class);
        prettifyOutputJob.setOutputKeyClass(NullWritable.class);
        prettifyOutputJob.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(prettifyOutputJob, new Path(statsCalculationOutputDir));
        TextOutputFormat.setOutputPath(prettifyOutputJob, new Path(finalOutputDir));
        TextInputFormat.setInputDirRecursive(prettifyOutputJob, true);
        TextOutputFormat.setCompressOutput(prettifyOutputJob, true);
        TextOutputFormat.setOutputCompressorClass(prettifyOutputJob, GzipCodec.class);
        LazyOutputFormat.setOutputFormatClass(prettifyOutputJob, TextOutputFormat.class);

        ret = prettifyOutputJob.waitForCompletion(true);
        if (!ret) return -1;
        return 0;
    }


}
