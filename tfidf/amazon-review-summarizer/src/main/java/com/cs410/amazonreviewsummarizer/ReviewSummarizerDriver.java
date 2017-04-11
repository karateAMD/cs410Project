package com.cs410.amazonreviewsummarizer;

import com.cs410.amazonreviewsummarizer.TFIDF0MakeDocuments.MakeDocumentsMapper;
import com.cs410.amazonreviewsummarizer.TFIDF0MakeDocuments.MakeDocumentsReducer;
import com.cs410.amazonreviewsummarizer.CombineInputFiles.CFInputFormat;
import com.cs410.amazonreviewsummarizer.TFIDF1WordCount.WordCountMapper;
import com.cs410.amazonreviewsummarizer.TFIDF1WordCount.WordCountReducer;
import com.cs410.amazonreviewsummarizer.TFIDF2WordCountPerDoc.WordCountPerDocMapper;
import com.cs410.amazonreviewsummarizer.TFIDF2WordCountPerDoc.WordCountPerDocReducer;
import com.cs410.amazonreviewsummarizer.TFIDF3DocCountPerWord.DocCountPerWordMapper;
import com.cs410.amazonreviewsummarizer.TFIDF3DocCountPerWord.DocCountPerWordReducer;
import com.cs410.amazonreviewsummarizer.TFIDF4PrettifyOutput.*;
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
public class ReviewSummarizerDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new ReviewSummarizerDriver(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String outputRoot = args[1];

        // intermediate output directories
        String makeDocsOutputDir = "/intermediate_results/tfidf_0_docs/";
        String wordCountOutputDir = "/intermediate_results/tfidf_1_wordcounts/";
        String wcPerDocOutputDir = "/intermediate_results/tfidf_2_wordcountsPerDoc/";
        String docCountPerWordOutputDir = "/intermediate_results/tfidf_3_docCountPerWord/";
        String finalOutputDir = outputRoot + "/results/";

        conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.95");

        // remove output dirs
        Runtime.getRuntime().exec("hadoop fs -rm -r /intermediate_results/").waitFor();

        // Job 0: MakeDocuments makes documents by combining all reviews for a product's good reviews
        // into one document and combining a product's bad reviews into a different document.
        // It does this for all products
        Job makeDocsJob = Job.getInstance(conf);
        makeDocsJob.setJobName("TFIDF 0: MakeDocuments");
        makeDocsJob.setNumReduceTasks(30);

        makeDocsJob.setJarByClass(ReviewSummarizerDriver.class);
        makeDocsJob.setMapperClass(MakeDocumentsMapper.class);
        makeDocsJob.setReducerClass(MakeDocumentsReducer.class);
        makeDocsJob.setMapOutputKeyClass(Text.class);
        makeDocsJob.setMapOutputValueClass(Text.class);
        makeDocsJob.setOutputKeyClass(NullWritable.class);
        makeDocsJob.setOutputValueClass(Text.class);

        TextInputFormat.setInputDirRecursive(makeDocsJob, true);
        TextOutputFormat.setCompressOutput(makeDocsJob, true);
        TextOutputFormat.setOutputCompressorClass(makeDocsJob, GzipCodec.class);
        TextInputFormat.addInputPath(makeDocsJob, new Path(args[0]));
        TextOutputFormat.setOutputPath(makeDocsJob, new Path(makeDocsOutputDir));
        MultipleOutputs.addNamedOutput(makeDocsJob, "tfidf0output", TextOutputFormat.class,
                NullWritable.class, Text.class);
        LazyOutputFormat.setOutputFormatClass(makeDocsJob, TextOutputFormat.class);


        boolean ret = makeDocsJob.waitForCompletion(true);
        if (!ret) return -1;

        // set TOTAL_DOCS in Configuration
        Long total_docs = makeDocsJob.getCounters().findCounter(GLOBAL_COUNTER.TOTAL_DOCS).getValue();
        conf.set("TOTAL_DOCS", total_docs.toString());
        System.out.println("TOTAL_DOCS="+conf.get("TOTAL_DOCS"));

        // Job 1: WordCount takes the product-binaryRating doc and
        // outputs {(word,docId) => wordCount}
        Job wordCountJob = Job.getInstance(conf);
        wordCountJob.setJobName("TFIDF 1: WordCount");
        
        wordCountJob.setJarByClass(ReviewSummarizerDriver.class);
        wordCountJob.setMapperClass(WordCountMapper.class);
        wordCountJob.setCombinerClass(WordCountReducer.class);
        wordCountJob.setReducerClass(WordCountReducer.class);
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputDirRecursive(wordCountJob, true);
        FileInputFormat.addInputPath(wordCountJob, new Path(makeDocsOutputDir));
        wordCountJob.setInputFormatClass(CFInputFormat.class);
        FileOutputFormat.setOutputPath(wordCountJob, new Path(wordCountOutputDir));
        FileOutputFormat.setCompressOutput(wordCountJob, true);
        FileOutputFormat.setOutputCompressorClass(wordCountJob, GzipCodec.class);
        LazyOutputFormat.setOutputFormatClass(wordCountJob, TextOutputFormat.class);

        ret = wordCountJob.waitForCompletion(true);
        if (!ret) return -1;
        /*else {
            System.out.println("Removing " + makeDocsOutputDir);
            Runtime.getRuntime().exec("hadoop fs -rm -r " + makeDocsOutputDir).waitFor();
        }*/

        // Job 2: WordCountPerDoc, takes {(word,docId) => wordCount} and
        // outputs {(word,docId) => (wordCount, wordsPerDoc)}
        Job wordcountPerDocJob = Job.getInstance(conf);
        wordcountPerDocJob.setJobName("TFIDF 2: WordCount per Doc");

        wordcountPerDocJob.setJarByClass(ReviewSummarizerDriver.class);
        wordcountPerDocJob.setMapperClass(WordCountPerDocMapper.class);
        wordcountPerDocJob.setReducerClass(WordCountPerDocReducer.class);
        wordcountPerDocJob.setOutputKeyClass(Text.class);
        wordcountPerDocJob.setOutputValueClass(Text.class);

        FileInputFormat.setInputDirRecursive(wordcountPerDocJob, true);
        FileInputFormat.addInputPath(wordcountPerDocJob, new Path(wordCountOutputDir));
        FileOutputFormat.setOutputPath(wordcountPerDocJob, new Path(wcPerDocOutputDir));
        FileOutputFormat.setCompressOutput(wordcountPerDocJob, true);
        FileOutputFormat.setOutputCompressorClass(wordcountPerDocJob, GzipCodec.class);
        LazyOutputFormat.setOutputFormatClass(wordcountPerDocJob, TextOutputFormat.class);

        ret = wordcountPerDocJob.waitForCompletion(true);
        if (!ret) return -1;
        /*else {
            System.out.println("Removing " + wordCountOutputDir);
            Runtime.getRuntime().exec("hadoop fs -rm -r " + wordCountOutputDir).waitFor();
        }*/

        // Job 3: DocCountPerWord takes {(word,docId) => (wordCount,wordsPerDoc)} and
        // outputs {(word,docId) => (wordCount, wordsPerDoc, docsPerWord)}
        conf.set("mapreduce.reduce.java.opts", "-Xmx4g");
        conf.set("mapreduce.reduce.memory.mb", "5000");

        Job docCountPerWordJob = Job.getInstance(conf);
        docCountPerWordJob.setJobName("TFIDF 3: Doc Count per Word");

        docCountPerWordJob.setJarByClass(ReviewSummarizerDriver.class);
        docCountPerWordJob.setMapperClass(DocCountPerWordMapper.class);
        docCountPerWordJob.setReducerClass(DocCountPerWordReducer.class);
        docCountPerWordJob.setOutputKeyClass(Text.class);
        docCountPerWordJob.setOutputValueClass(Text.class);

        FileInputFormat.setInputDirRecursive(docCountPerWordJob, true);
        FileInputFormat.addInputPath(docCountPerWordJob, new Path(wcPerDocOutputDir));
        FileOutputFormat.setOutputPath(docCountPerWordJob, new Path(docCountPerWordOutputDir));
        FileOutputFormat.setCompressOutput(docCountPerWordJob, true);
        FileOutputFormat.setOutputCompressorClass(docCountPerWordJob, GzipCodec.class);
        MultipleOutputs.addNamedOutput(docCountPerWordJob, "tfidf3output", TextOutputFormat.class,
                Text.class, Text.class);
        LazyOutputFormat.setOutputFormatClass(docCountPerWordJob, TextOutputFormat.class);

        ret = docCountPerWordJob.waitForCompletion(true);
        if (!ret) return -1;
        /*else {
            System.out.println("Removing " + wcPerDocOutputDir);
            Runtime.getRuntime().exec("hadoop fs -rm -r " + wcPerDocOutputDir).waitFor();
        }*/

        // Job 4: PrettifyOutput creates json output
        Job prettifyOutputJob = Job.getInstance(conf);
        prettifyOutputJob.setJobName("TFIDF 4: Prettify Output");

        prettifyOutputJob.setJarByClass(ReviewSummarizerDriver.class);
        prettifyOutputJob.setMapperClass(PrettifyOutputMapper.class);
        prettifyOutputJob.setReducerClass(PrettifyOutputReducer.class);
        prettifyOutputJob.setMapOutputKeyClass(PrettifyOutputCompositeKey.class);
        prettifyOutputJob.setMapOutputValueClass(Text.class);
        prettifyOutputJob.setOutputKeyClass(NullWritable.class);
        prettifyOutputJob.setOutputValueClass(Text.class);

        prettifyOutputJob.setPartitionerClass(PrettifyOutputPartitioner.class);
        prettifyOutputJob.setGroupingComparatorClass(PrettifyOutputGrouper.class);
        prettifyOutputJob.setSortComparatorClass(PrettifyOutputSorter.class);

        FileInputFormat.setInputDirRecursive(prettifyOutputJob, true);
        FileInputFormat.addInputPath(prettifyOutputJob, new Path(docCountPerWordOutputDir));
        FileOutputFormat.setOutputPath(prettifyOutputJob, new Path(finalOutputDir));
        FileOutputFormat.setCompressOutput(prettifyOutputJob, true);
        FileOutputFormat.setOutputCompressorClass(prettifyOutputJob, GzipCodec.class);
        LazyOutputFormat.setOutputFormatClass(prettifyOutputJob, TextOutputFormat.class);

        ret = prettifyOutputJob.waitForCompletion(true);
        if (!ret) return -1;
        /*else {
            System.out.println("Removing " + docCountPerWordOutputDir);
            Runtime.getRuntime().exec("hadoop fs -rm -r " + docCountPerWordOutputDir).waitFor();
        }*/

        return 0;
    }


}
