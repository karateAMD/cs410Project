package com.cs410.summarystats;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

/**
 * Created by angelading on 4/15/17.
 */
public class SummaryStatsMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Logger log = Logger.getLogger(SummaryStatsMapper.class);
    private Text productID = new Text();
    private Text stats = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /* Each line has format { "asin": "1384719342", "reviewText": "body of review", "overall": 5.0 }
            where asin = product ID, reviewText = the body of the review, and overall = the rating given to the product
         */
        try {
            JSONObject json = new JSONObject(value.toString());
            String prodID = json.getString("asin");
            double rating = json.getDouble("overall");
            int numReviews = 1;
            int[] ratingCounts = {0,0,0,0,0};

            // Determine if rating is a 1, 2, 3, 4, or 5.
            // Assign a count of "1" to that rating's element in the array.
            if (rating == 1)
                ratingCounts[0] = 1;
            else if (rating == 2)
                ratingCounts[1] = 1;
            else if (rating == 3)
                ratingCounts[2] = 1;
            else if (rating == 4)
                ratingCounts[3] = 1;
            else
                ratingCounts[4] = 1;

            String ratingCountsStr = ratingCounts[0]+"-"+ratingCounts[1]+"-"+ratingCounts[2]+"-"
                                    +ratingCounts[3]+"-"+ratingCounts[4];

            productID.set(prodID);
            stats.set(rating+","+numReviews+","+ratingCountsStr);
            System.out.println("Output: "+stats.toString());
            context.write(productID, stats);
        } catch (Exception e) {
            log.error(e.getStackTrace());
        }
    }

}
