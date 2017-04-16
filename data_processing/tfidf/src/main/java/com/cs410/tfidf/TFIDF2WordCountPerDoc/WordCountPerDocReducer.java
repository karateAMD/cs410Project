package com.cs410.tfidf.TFIDF2WordCountPerDoc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by angelading on 3/23/17.
 */
public class WordCountPerDocReducer extends Reducer<Text, Text, Text, Text> {
    private Text word_docId = new Text();
    private Text wordcount_wordsPerDoc = new Text();
    HashMap<String, Integer> word_wordcount = new HashMap<String, Integer>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // input format: docId => [(word1,wc1), (word2,wc2), (word3,wc3), . . . ]
        // output format: (word,docId) => (wordcount, wordsPerDoc)
        int wordsPerDoc = 0;
        String docId = key.toString();
        word_wordcount.clear();

        for (Text wordAndWordcount : values) {
            // save word and wordcount in word_wordcount temporary map
            String[] pair = wordAndWordcount.toString().split(",");
            String word = pair[0];
            Integer wordcount = Integer.parseInt(pair[1]);
            word_wordcount.put(word, wordcount);

            // accumulate total count of words in this reduce's document
            wordsPerDoc += wordcount;
        }

        // output: (word,docId) => (wordcount, wordsPerDoc)
        for (String word : word_wordcount.keySet()) {
            Integer wordcount = word_wordcount.get(word);
            word_docId.set(word + "," + docId);
            wordcount_wordsPerDoc.set(wordcount + "," + wordsPerDoc);
            context.write(word_docId, wordcount_wordsPerDoc);
        }
    }
}
