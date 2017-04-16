package com.cs410.tfidf.TFIDF0MakeDocuments;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by angelading on 3/15/17.
 */
public class MakeDocumentsTest {
    // https://cwiki.apache.org/confluence/display/MRUNIT/MRUnit+Tutorial
    MapDriver<LongWritable, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, NullWritable, Text> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, Text, NullWritable, Text> mapReduceDriver;

    @Before
    public void setUp() throws Exception {
        MakeDocumentsMapper mapper = new MakeDocumentsMapper();
        MakeDocumentsReducer reducer = new MakeDocumentsReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                "{\"reviewerID\": \"A2IBPI20UZIR0U\", \"asin\": \"1384719342\", \"reviewerName\": \"cassandra tu\", \"helpful\": [0, 0], \"reviewText\": \"Not much to write about here, but it does exactly what it's supposed to. filters out the pop sounds. now my recordings are much more crisp. it is one of the lowest prices pop filters on amazon so might as well buy it, they honestly work the same despite their pricing,\", \"overall\": 5.0, \"summary\": \"good\", \"unixReviewTime\": 1393545600, \"reviewTime\": \"02 28, 2014\"}"));
        mapDriver.withOutput(new Text("1384719342_good"), new Text("Not much to write about here, but it does exactly what it's supposed to. filters out the pop sounds. now my recordings are much more crisp. it is one of the lowest prices pop filters on amazon so might as well buy it, they honestly work the same despite their pricing,"));
        mapDriver.runTest();
    }

    @After
    public void tearDown() throws Exception {


    }
}