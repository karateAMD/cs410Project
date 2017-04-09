package com.cs410.amazonreviewsummarizer.TFIDF4PrettifyOutput;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by angelading on 3/24/17.
 */
public class PrettifyOutputCompositeKey implements WritableComparable<PrettifyOutputCompositeKey> {
    private String productID;
    private Integer binaryRating;
    private Double tfidf;

    public PrettifyOutputCompositeKey() {
        super();
    }

    public void set(String productID, Integer binaryRating, Double tfidf) {
        this.productID = productID;
        this.binaryRating = binaryRating;
        this.tfidf = tfidf;
    }

    @Override
    public int hashCode() {
        int result = productID.hashCode();
        result = 31 * result + binaryRating.hashCode();
        result = 31 * result + tfidf.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        PrettifyOutputCompositeKey other = (PrettifyOutputCompositeKey) obj;

        if (!this.productID.equals(other.productID) ||
                !this.binaryRating.equals(other.productID) ||
                !this.tfidf.equals(other.tfidf))
            return false;

        return true;
    }

    @Override
    public int compareTo(PrettifyOutputCompositeKey o) {
        int compareResult = productID.compareTo(o.productID);
        if (compareResult != 0) return compareResult;

        compareResult = binaryRating.compareTo(o.binaryRating);
        if (compareResult != 0) return compareResult;

        return -1 * tfidf.compareTo(o.tfidf);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(productID);
        dataOutput.writeInt(binaryRating);
        dataOutput.writeDouble(tfidf);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        productID = dataInput.readUTF();
        binaryRating = dataInput.readInt();
        tfidf = dataInput.readDouble();
    }



    public String getProductID() {
        return productID;
    }

    public Integer getBinaryRating() {
        return binaryRating;
    }

    public Double getTfidf() {
        return tfidf;
    }
}
