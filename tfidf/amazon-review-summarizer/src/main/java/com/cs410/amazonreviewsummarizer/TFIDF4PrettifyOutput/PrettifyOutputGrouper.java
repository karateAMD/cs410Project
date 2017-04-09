package com.cs410.amazonreviewsummarizer.TFIDF4PrettifyOutput;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by angelading on 3/24/17.
 */
public class PrettifyOutputGrouper extends WritableComparator {
    // partition&group by: productID

    public PrettifyOutputGrouper() {
        super(PrettifyOutputCompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        PrettifyOutputCompositeKey key1 = (PrettifyOutputCompositeKey) a;
        PrettifyOutputCompositeKey key2 = (PrettifyOutputCompositeKey) b;

        return key1.getProductID().compareTo(key2.getProductID());
    }
}
