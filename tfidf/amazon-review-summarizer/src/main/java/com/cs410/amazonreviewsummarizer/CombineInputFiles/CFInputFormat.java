package com.cs410.amazonreviewsummarizer.CombineInputFiles;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;

import java.io.IOException;

/**
 * Created by angelading on 4/7/17.
 */
public class CFInputFormat extends CombineFileInputFormat<CFFileLineWritable, Text> {

    public CFInputFormat(){
        super();
        setMaxSplitSize(1*1024*1024); // 64 MB, default block size on hadoop
    }

    @Override
    public RecordReader<CFFileLineWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException{
        return new CombineFileRecordReader<>((CombineFileSplit)inputSplit, context, CFRecordReader.class);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file){
        return false;
    }

}
