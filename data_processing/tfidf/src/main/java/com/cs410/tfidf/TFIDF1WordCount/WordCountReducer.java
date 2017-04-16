package com.cs410.tfidf.TFIDF1WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * Created by angelading on 3/5/17.
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable count = new IntWritable();

    public void reduce(Text word_and_docId, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        count.set(sum);
        context.write(word_and_docId, count); // [word_docID]\t[sum]
    }
}
