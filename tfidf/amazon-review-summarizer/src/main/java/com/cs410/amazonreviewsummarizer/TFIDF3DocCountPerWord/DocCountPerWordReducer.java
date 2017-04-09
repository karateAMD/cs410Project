package com.cs410.amazonreviewsummarizer.TFIDF3DocCountPerWord;

import com.cs410.amazonreviewsummarizer.GLOBAL_COUNTER;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by angelading on 3/23/17.
 */
public class DocCountPerWordReducer extends Reducer<Text, Text, NullWritable, Text> {
    private Long totalDocs;
    private Text word_tfIdf = new Text();
    private MultipleOutputs<NullWritable,Text> outputs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outputs = new MultipleOutputs<>(context);
        Configuration conf = context.getConfiguration();
        totalDocs = Long.parseLong(conf.get("TOTAL_DOCS"));
        System.out.println("TOTAL_DOCS = "+totalDocs);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // input:  word => (docId, wordCount, wordsPerDoc)
        // output: (word, docId)  =>  tf-idf
        // where docId=productID_binaryReview

        int docsPerWord = 0;
        String word = key.toString();
        HashMap<String, Integer[]> docId_wcAndWpds = new HashMap<>();

        // save all the reducer values (docId, wordcount, wordsPerDoc) while counting total docs for this
        // reduce's word
        for (Text val : values) {
            String[] docId_wordCount_wordsPerDoc = val.toString().split(",");
            String docId = docId_wordCount_wordsPerDoc[0];
            Integer[] wordcount_wordsPerDoc = {Integer.parseInt(docId_wordCount_wordsPerDoc[1]),
                    Integer.parseInt(docId_wordCount_wordsPerDoc[2])};
            docId_wcAndWpds.put(docId, wordcount_wordsPerDoc);
            docsPerWord += 1;
        }

        // for each word-docId, calculate its TF-IDF
        for (String docId : docId_wcAndWpds.keySet()) {
            Integer[] wordCount_wordsPerDoc = docId_wcAndWpds.get(docId);
            double wordCount = (double)wordCount_wordsPerDoc[0].intValue();
            double wordsPerDoc = (double)wordCount_wordsPerDoc[1].intValue();
            double tfidf = (wordCount / wordsPerDoc) * Math.log(totalDocs / docsPerWord);
            word_tfIdf.set(word + "," + tfidf); // "word,tfidf"
            outputs.write("tfidf3output", NullWritable.get(), word_tfIdf, docId);
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        outputs.close();
    }
}
