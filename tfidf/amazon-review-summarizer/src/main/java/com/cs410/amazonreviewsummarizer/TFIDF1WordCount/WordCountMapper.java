package com.cs410.amazonreviewsummarizer.TFIDF1WordCount;

import com.cs410.amazonreviewsummarizer.CombineInputFiles.CFFileLineWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Created by angelading on 3/5/17.
 */
public class WordCountMapper extends Mapper<CFFileLineWritable, Text, Text, IntWritable> {
    private FileSystem fs = null;
    private final static IntWritable one = new IntWritable(1);
    private Text word_and_docId = new Text();

    public static final String[] stopwords = new String[] {"a","about","above","after","again","against","all","am","an","and","any","are","aren't","as","at","be","because","been","before","being","below","between","both","but","by","can't","cannot","could","couldn't","did","didn't","do","does","doesn't","doing","don't","down","during","each","few","for","from","further","had","hadn't","has","hasn't","have","haven't","having","he","he'd","he'll","he's","her","here","here's","hers","herself","him","himself","his","how","how's","i","i'd","i'll","i'm","i've","if","in","into","is","isn't","it","it's","its","itself","let's","me","more","most","mustn't","my","myself","no","nor","not","of","off","on","once","only","or","other","ought","our","ours	ourselves","out","over","own","same","shan't","she","she'd","she'll","she's","should","shouldn't","so","some","such","than","that","that's","the","their","theirs","them","themselves","then","there","there's","these","they","they'd","they'll","they're","they've","this","those","through","to","too","under","until","up","very","was","wasn't","we","we'd","we'll","we're","we've","were","weren't","what","what's","when","when's","where","where's","which","while","who","who's","whom","why","why's","with","won't","would","wouldn't","you","you'd","you'll","you're","you've","your","yours","yourself","yourselves","i'm","want","wanted","what","use","using" };
    public static final Set<String> STOPWORDS = new HashSet<>(Arrays.asList(stopwords));

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    public void map(CFFileLineWritable key, Text value, Context context) throws IOException, InterruptedException {
        // get just the "productID_binaryReview" string from the file path
        Path filePath = new Path(key.fileName); //((FileSplit) context.getInputSplit()).getPath().getName();
        if (fs == null) fs = filePath.getFileSystem(context.getConfiguration());
        String fileName = filePath.getName();
        String docId = fileName.substring(0, fileName.indexOf("-r-"));
        BufferedReader bf = getFile(filePath);
        if (bf == null) return;
        String line;
        while ((line = bf.readLine()) != null) {
            // remove all non-alpha or non-apostrophe characters from the word
            String[] words = line.split(" ");
            for (String w : words) {
                String word = w.toLowerCase().replaceAll("[^a-zA-Z']", "");
                System.out.println(word);

                // output word if it isnt a stopword
                if (!STOPWORDS.contains(word) && word.length() > 0) {
                    word_and_docId.set(word + "," + docId);
                    context.write(word_and_docId, one);
                }
            }
        }
        String unzippedFileName = fileName.substring(0, fileName.length()-3);
        Runtime.getRuntime().exec("sudo rm "+unzippedFileName);
        bf.close();
    }

    private BufferedReader getFile(Path filePath) throws IOException, InterruptedException{
        String fileName = filePath.getName();
        fs.copyToLocalFile(filePath, new Path(fileName));

        boolean isSuccess = Runtime.getRuntime().exec("gunzip "+fileName).waitFor() == 0;
        if (!isSuccess) {
            System.out.println("Unzip no work");
            return null;
        }
        String unzippedFileName = fileName.substring(0, fileName.length()-3);
        return new BufferedReader(new FileReader(unzippedFileName));
    }
}
