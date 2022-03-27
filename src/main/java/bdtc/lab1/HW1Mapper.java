package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Stream;


public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final Text word = new Text();
    private final Pattern digitCodePattern = Pattern.compile("[\\d]+-[\\d]+-[\\d]+:");
    private final Pattern linuxCodeLog = Pattern.compile("-[0-7]-");
    private HashMap<String, String> wordDefineCode = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        wordDefineCode = new HashMap<>();

        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            try (Stream<String> stream = Files.lines(Paths.get(cacheFiles[0]))) {
                stream.forEach((line) -> {
                    String[] splinted = line.split("-", 2);
                    wordDefineCode.put(splinted[0], splinted[1]);
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

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
                word.set(wordDefineCode.get(linuxCode));
                context.write(word, one);
            }
        }
    }
}
