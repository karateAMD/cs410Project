package com.cs410.amazonreviewsummarizer.TFIDF4PrettifyOutput;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by angelading on 3/24/17.
 */
public class PrettifyOutputMapper extends Mapper<LongWritable, Text, PrettifyOutputCompositeKey, Text> {
    // map input filename: productID_binaryRating-r-00001.gz
    // line in file:  word,tf-idf
    // map input:  productID, binaryRating, word, tfidf
    // map output: productID(,binaryRating,tfidf)  => binaryRating, word, tfidf

    private PrettifyOutputCompositeKey compositeKey = new PrettifyOutputCompositeKey();
    private Text binaryRating_word_tfidf = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // get productId and binaryRating from filename
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        String[] docId = fileName.substring(0, fileName.indexOf("-r-")).split("_");
        String productId = docId[0];
        int binaryRating = Integer.parseInt(docId[1]);

        // get word and tfidf from line in file
        String word_tfidf = value.toString();
        double tfidf = Double.parseDouble(word_tfidf.split(",")[1]);

        // create output key used for secondary sorting, and output value
        compositeKey.set(productId, binaryRating, tfidf);
        binaryRating_word_tfidf.set(binaryRating + "," + word_tfidf);

        // output key and value
        context.write(compositeKey, binaryRating_word_tfidf);
    }
}