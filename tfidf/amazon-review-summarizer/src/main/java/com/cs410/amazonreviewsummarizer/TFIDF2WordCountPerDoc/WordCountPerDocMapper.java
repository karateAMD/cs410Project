package com.cs410.amazonreviewsummarizer.TFIDF2WordCountPerDoc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by angelading on 3/23/17.
 */
public class WordCountPerDocMapper extends Mapper<LongWritable,Text,Text,Text> {
    private Text docId = new Text();
    private Text wordAndWordcount = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("Setup");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // input:  (docId,word) => wordcount,        e.g. "B000068NTU_good,to	10"
        // output: docId        => (word,wordcount)

        String line = value.toString();
        String[] pair = line.split("\t");
        String[] docIDandWord = pair[0].split(",");
        String word = docIDandWord[0];
        String docId_s = docIDandWord[1];
        String wordcount = pair[1];
        docId.set(docId_s);
        wordAndWordcount.set(word+","+wordcount);
        System.out.println("docId="+docId+", word="+word+", wordcount="+wordcount);
        context.write(docId, wordAndWordcount);
    }
}
