package mapreduce.wordcount;

import java.util.Arrays;

import mapreduce.wordcount.WordCountReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Test;

public class WordCountReducerTest {

    @Test
    public void testReducer() {
        // 1. 설정
        ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver = new ReduceDriver();
        reduceDriver.withReducer(new WordCountReducer());
        reduceDriver.withInputKey(new Text("World"));
        reduceDriver.withInputValues(Arrays.asList(new IntWritable(1), new IntWritable(1)));

        // 2. 검증 및 실행
        reduceDriver.withOutput(new Text("World"), new IntWritable(2));
        reduceDriver.runTest();
    }
}