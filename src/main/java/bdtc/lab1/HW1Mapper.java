package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.*;


public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final Text word = new Text();
    private final Pattern digitCodePattern = Pattern.compile("[\\d]+-[\\d]+-[\\d]+:");
    private final Pattern linuxCodeLog = Pattern.compile("-[0-7]-");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Matcher matcher = digitCodePattern.matcher(line);

        if (!matcher.find()) {
            context.getCounter(CounterType.MALFORMED).increment(1);
        } else {
            String partLog = matcher.group();
            Matcher codeMatcher = linuxCodeLog.matcher(partLog);

            if (!codeMatcher.find()) {
                context.getCounter(CounterType.MALFORMED).increment(1);
            } else {
                String linuxCode = codeMatcher.group().replaceAll("[-]", "");
                word.set(linuxCode);
                context.write(word, one);
            }
        }
    }
}
