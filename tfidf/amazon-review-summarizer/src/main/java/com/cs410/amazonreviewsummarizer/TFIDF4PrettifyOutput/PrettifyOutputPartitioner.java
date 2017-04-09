package com.cs410.amazonreviewsummarizer.TFIDF4PrettifyOutput;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by angelading on 3/24/17.
 */
public class PrettifyOutputPartitioner extends Partitioner<PrettifyOutputCompositeKey, Text> {
    // partition&group by: productID

    @Override
    public int getPartition(PrettifyOutputCompositeKey key, Text value, int numReducers) {
        return (Math.abs(key.getProductID().hashCode()) & Integer.MAX_VALUE) % numReducers;
    }
}
