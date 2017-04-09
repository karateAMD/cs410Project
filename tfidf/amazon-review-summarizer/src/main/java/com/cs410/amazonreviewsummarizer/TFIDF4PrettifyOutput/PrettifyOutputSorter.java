package com.cs410.amazonreviewsummarizer.TFIDF4PrettifyOutput;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by angelading on 3/24/17.
 */
// sort by: ascending productID, ascending binaryRating, descending tfidf
public class PrettifyOutputSorter extends WritableComparator {

    public PrettifyOutputSorter() { super(PrettifyOutputCompositeKey.class, true); }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        PrettifyOutputCompositeKey key1 = (PrettifyOutputCompositeKey) a;
        PrettifyOutputCompositeKey key2 = (PrettifyOutputCompositeKey) b;

        return key1.compareTo(key2);
    }
}
