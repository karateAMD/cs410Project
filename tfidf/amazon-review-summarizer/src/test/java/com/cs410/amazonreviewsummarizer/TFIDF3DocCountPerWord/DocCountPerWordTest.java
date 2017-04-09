package com.cs410.amazonreviewsummarizer.TFIDF3DocCountPerWord;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.mrunit.testutil.ExtendedAssert.assertListEquals;
import static org.junit.Assert.*;

/**
 * Created by angelading on 3/23/17.
 */
public class DocCountPerWordTest {
    DocCountPerWordMapper mapper = new DocCountPerWordMapper();
    MapDriver<LongWritable,Text,Text,Text> mapDriver = MapDriver.newMapDriver(mapper);

    @Test
    public void testMap() throws Exception {
        //word_docId, wordcount_wordsPerDoc
        List<Pair<Text, Text>> actual = mapDriver.withInput(new LongWritable(0),
                new Text("word,docId\t3,10")).run();
        List<Pair<Text, Text>> expected = new ArrayList<>();
        expected.add(new Pair<>(new Text("word"), new Text("docId,3,10")));
        assertListEquals(expected, actual);
    }

    @Test
    public void testReducer() throws Exception {
        Double wordCount = (double)1;
        Double wordsPerDoc = (double)169;
        int totalDocs = 8;
        int docsPerWord = 1;
        double tfidf = (wordCount / wordsPerDoc) * Math.log(totalDocs / docsPerWord);
        System.out.println(Math.log(8)/169);
    }
}