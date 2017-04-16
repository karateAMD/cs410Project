package com.cs410.tfidf.TFIDF3DocCountPerWord;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by angelading on 3/23/17.
 */
public class DocCountPerWordMapper extends Mapper<LongWritable,Text,Text,Text> {
    private Text word = new Text();
    private Text docId_wordCount_wordsPerDoc = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] keyVal = value.toString().split("\t");
        String[] word_docId = keyVal[0].split(",");
        String[] wordCount_wordsPerDoc = keyVal[1].split(",");
        String wordCount = wordCount_wordsPerDoc[0];
        String wordsPerDoc = wordCount_wordsPerDoc[1];

        word.set(word_docId[0]);
        docId_wordCount_wordsPerDoc.set(word_docId[1] + "," + wordCount + "," + wordsPerDoc);
        context.write(word, docId_wordCount_wordsPerDoc);
    }
}
