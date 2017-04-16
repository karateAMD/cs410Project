package com.cs410.tfidf.TFIDF2WordCountPerDoc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.mrunit.testutil.ExtendedAssert.assertListEquals;
import static org.junit.Assert.*;

/**
 * Created by angelading on 3/23/17.
 */
public class WordCountPerDocTest {
    WordCountPerDocMapper mapper = new WordCountPerDocMapper();
    WordCountPerDocReducer reducer = new WordCountPerDocReducer();
    MapDriver<LongWritable,Text,Text,Text> mapDriver = MapDriver.newMapDriver(mapper);
    ReduceDriver<Text, Text, Text, Text> reduceDriver = ReduceDriver.newReduceDriver(reducer);

    @Test
    public void testMap() throws Exception {
        List<Pair<Text, Text>> actual = mapDriver.withInput(new LongWritable(0), new Text("word,docId\t7")).run();
        List<Pair<Text, Text>> expected = new ArrayList<>();
        expected.add(new Pair<>(new Text("docId"), new Text("word,7")));
        assertListEquals(expected, actual);
    }

    @Test
    public void testReduce() throws Exception {
        List<Pair<Text, Text>> outList = null;

        List<Text> values = new ArrayList<>();
        values.add(new Text("word1,1"));
        values.add(new Text("word2,2"));
        values.add(new Text("word3,3"));
        outList = reduceDriver.withInput(new Text("docId"), values).run();

        Set<Pair<String,String>> outSet = new HashSet<>();
        for (Pair p : outList) {
            String k = p.getFirst().toString();
            String v = p.getSecond().toString();
            outSet.add(new Pair<>(k,v));
        }

        Set<Pair<String, String>> expected = new HashSet<>();
        expected.add(new Pair<>("word1,docId", "1,6"));
        expected.add(new Pair<>("word2,docId", "2,6"));
        expected.add(new Pair<>("word3,docId", "3,6"));

        assertTrue(expected.equals(outSet));
    }
}