package com.cs410.summarystats;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by angelading on 4/15/17.
 */
public class SummaryStatsReducer extends Reducer<Text, Text, Text, Text> {
    Text nextRoundOfStats = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // meanRating,numReviews,ratingCounts
        double ratingsSum = 0.;
        int totalNumRatings = 0;
        int[] groupCountsAcc = {0,0,0,0,0};

        for (Text val : values) {
            String[] stats = values.toString().split(",");
            double meanRating = Double.parseDouble(stats[0]);
            int numRatings = Integer.parseInt(stats[1]);

            // parse ratingCounts into an array of ints
            String[] ratingCountsStr = stats[3].split("-");
            int[] ratingCounts = new int[5];
            for (int i = 0; i < ratingCountsStr.length; i++) {
                ratingCounts[i] = Integer.parseInt(ratingCountsStr[i]);
            }

            // compute aggregates
            ratingsSum += meanRating * numRatings;
            totalNumRatings ++;
            for (int i = 0; i < ratingCountsStr.length; i++) {
                groupCountsAcc[i] += ratingCounts[i];
            }
        }

        double newMeanRating = ratingsSum / totalNumRatings;
        String ratingCountsStr = groupCountsAcc[0]+"-"+groupCountsAcc[1]+"-"+groupCountsAcc[2]+"-"
                +groupCountsAcc[3]+"-"+groupCountsAcc[4];

        nextRoundOfStats.set(newMeanRating+","+totalNumRatings+","+ratingCountsStr);
        context.write(key, nextRoundOfStats);
    }

}
