package com.cs410.summarystats;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Created by angelading on 4/15/17.
 */
class PrettifyOutputMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private Text output = new Text();
    private boolean isFirstLineInOutputFile = true;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // INPUT: productID -> meanRating,totalNumReviews,numReviewsPerLevel
        String[] productID_stats = value.toString().split("\t");
        String productID = productID_stats[0];
        String rest = productID_stats[1];
        JSONObject productObject = new JSONObject();
        String[] stats = rest.split(",");
        System.out.println(stats);
        double meanRating = round(Double.parseDouble(stats[0]), 2);
        int totalNumReviews = Integer.parseInt(stats[1]);

        JSONObject numReviewsPerRatingLevel = new JSONObject();
        String[] temp = stats[2].split("-");

        try {
            for (int i = 0; i < temp.length; i++) {
                numReviewsPerRatingLevel.put(Integer.toString(i+1), Integer.parseInt(temp[i]));
            }

            productObject.put("productID", productID);
            productObject.put("meanRating", meanRating);
            productObject.put("numReviews", totalNumReviews);
            productObject.put("numReviewsPerRatingLevel", (Object)numReviewsPerRatingLevel);

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

    public static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        BigDecimal bd = new BigDecimal(value);
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }

}
