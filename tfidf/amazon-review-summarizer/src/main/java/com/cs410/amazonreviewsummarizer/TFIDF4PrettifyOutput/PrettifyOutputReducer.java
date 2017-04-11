package com.cs410.amazonreviewsummarizer.TFIDF4PrettifyOutput;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

/**
 * Created by angelading on 3/24/17.
 */
public class PrettifyOutputReducer extends Reducer<PrettifyOutputCompositeKey, Text, NullWritable, Text> {
    private static int MAX_WORDS_PER_RATING_GROUP = 50;
    // reduce input: productID => [(binaryRating, word, tfidf)]
    // reduct output: files with contents like below...
    /**********************************************
     {
     "productID": "0000013714",
     "goodRatingWords":
         {
             "word1": 0.00432,
             "word2": 0.00385,
             "word3": 0.00133
         },
     "badRatingWords":
         {
             "word1": 0.00599,
             "word2": 0.00401,
             "word3": 0.00182,
             "word4": 0.00122,
             "word5": 0.00039
         }
     },
    *********************************************/
    private Text output = new Text();
    private boolean isFirstLineInOutputFile = true;

    @Override
    protected void reduce(PrettifyOutputCompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        JSONObject productObject = new JSONObject();
        JSONObject goodRatingObject = new JSONObject();
        JSONObject badRatingObject = new JSONObject();
        int numGoodWords = 0;
        int numBadWords = 0;

        try {
            productObject.put("productID", key.getProductID());
            productObject.put("goodRatingWords", goodRatingObject);
            productObject.put("badRatingWords", badRatingObject);

            for (Text value : values) {
                System.out.println(value);
                String[] binaryRating_word_tfidf = value.toString().split(",", -1);
                int binaryRating = Integer.parseInt(binaryRating_word_tfidf[0]);
                String word = binaryRating_word_tfidf[1];
                double tfidf = Double.parseDouble(binaryRating_word_tfidf[2]);

                if (binaryRating == 0 && numGoodWords <= MAX_WORDS_PER_RATING_GROUP) {
                    // this word belongs to the good rating group
                    goodRatingObject.put(word, tfidf);
                    numGoodWords++;
                } else if (numBadWords <= MAX_WORDS_PER_RATING_GROUP) {
                    // this word belongs to the bad rating group
                    badRatingObject.put(word, tfidf);
                    numBadWords++;
                }
            }

            if (isFirstLineInOutputFile) {
                output.set(productObject.toString());
                isFirstLineInOutputFile = false;
            } else {
                output.set(",\n"+productObject.toString());
            }

            context.write(NullWritable.get(), output);

        } catch (JSONException e) {
            System.err.println(e.getStackTrace());
        }
    }
}
