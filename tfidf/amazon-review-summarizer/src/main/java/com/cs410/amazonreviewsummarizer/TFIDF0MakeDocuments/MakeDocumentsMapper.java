package com.cs410.amazonreviewsummarizer.TFIDF0MakeDocuments;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONObject;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * Created by angelading on 3/14/17.
 */
public class MakeDocumentsMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Logger log = Logger.getLogger(MakeDocumentsMapper.class);
    private Text productID_binaryRating = new Text();
    private Text reviewText = new Text();

    double binaryRatingSplitpoint = 4.0; // todo: make configurable

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /* Each line has format { "asin": "1384719342", "reviewText": "body of review", "overall": 5.0 }
            where asin = product ID, reviewText = the body of the review, and overall = the rating given to the product
         */
        try {
            JSONObject json = new JSONObject(value.toString());
            String productID = json.getString("asin");
            int binaryRating = json.getDouble("overall") < binaryRatingSplitpoint ? 0 : 1; // 0=bad, 1=good
            productID_binaryRating.set(productID + "_" + binaryRating);
            reviewText.set(json.getString("reviewText"));
            context.write(productID_binaryRating, reviewText);
        } catch (Exception e) {
            log.error(e.getStackTrace());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }

}
