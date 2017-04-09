package com.cs410.amazonreviewsummarizer.TFIDF1WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.mrunit.testutil.ExtendedAssert.assertListEquals;
import static org.junit.Assert.*;

/**
 * Created by angelading on 3/23/17.
 */
public class WordCountTest {
    WordCountMapper mapper = new WordCountMapper();
    WordCountReducer reducer = new WordCountReducer();
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver = MapDriver.newMapDriver(mapper);
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver = ReduceDriver.newReduceDriver(reducer);

    @Test
    public void testMapper() throws IOException {
        List<Pair<Text, IntWritable>> out = mapDriver.withInput(new LongWritable(0), new Text("foo")).run();
        List<Pair<Text, IntWritable>> expected = new ArrayList<>();
        expected.add(new Pair<>(new Text("foo"), new IntWritable(1)));

        //assertEquals(expected, out);
    }

    @Test
    public void testReducer() throws Exception {
        List<Pair<Text, IntWritable>> out = null;

        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(2));
        values.add(new IntWritable(3));
        out = reduceDriver.withInput(new Text("word1_doc1"), values).run();

        List<Pair<Text, IntWritable>> expected = new ArrayList<>();
        expected.add(new Pair<>(new Text("word1_doc1"), new IntWritable(6)));

        assertListEquals(expected, out);

    }
}