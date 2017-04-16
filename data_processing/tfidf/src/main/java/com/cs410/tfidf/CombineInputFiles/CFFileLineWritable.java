package com.cs410.tfidf.CombineInputFiles;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by angelading on 4/7/17.
 */
public class CFFileLineWritable implements WritableComparable<CFFileLineWritable>{
    public long offset;
    public String fileName;

    public void readFields(DataInput in) throws IOException {
        this.offset = in.readLong();
        this.fileName = Text.readString(in);
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(offset);
        Text.writeString(out, fileName);
    }

    public int compareTo(CFFileLineWritable that) {
        int cmp = this.fileName.compareTo(that.fileName);
        if (cmp != 0) return cmp;
        return (int)Math.signum((double)(this.offset - that.offset));
    }

    @Override
    public int hashCode() {               // generated hashCode()
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fileName == null) ? 0 : fileName.hashCode());
        result = prime * result + (int) (offset ^ (offset >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {  // generated equals()
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CFFileLineWritable other = (CFFileLineWritable) obj;
        if (fileName == null) {
            if (other.fileName != null)
                return false;
        } else if (!fileName.equals(other.fileName))
            return false;
        if (offset != other.offset)
            return false;
        return true;
    }
}