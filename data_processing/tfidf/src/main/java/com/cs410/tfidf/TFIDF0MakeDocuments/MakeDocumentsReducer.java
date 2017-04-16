package com.cs410.tfidf.TFIDF0MakeDocuments;

import com.cs410.tfidf.GLOBAL_COUNTER;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by angelading on 3/14/17.
 */
public class MakeDocumentsReducer extends Reducer<Text, Text, NullWritable, Text> {
    private MultipleOutputs outputs;
    private Configuration conf;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outputs = new MultipleOutputs<NullWritable, Text>(context);
        conf = context.getConfiguration();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // this function will create one file with all the reviews for a productID-binaryRating group
        for (Text reviewText : values) {
            outputs.write("tfidf0output", NullWritable.get(), reviewText, key.toString());
        }

        // increment global totalDocs counter
        context.getCounter(GLOBAL_COUNTER.TOTAL_DOCS).increment(1);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Long total_docs = context.getCounter(GLOBAL_COUNTER.TOTAL_DOCS).getValue();
        conf.set("TOTAL_DOCS", total_docs.toString());

        outputs.close();
        super.cleanup(context);
    }

}
