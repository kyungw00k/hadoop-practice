package mapreduce.wordcount;

import mapreduce.wordcount.WordCountMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Test;

/**
 * 테스트를 통하여 Mapper와 Reducer를 테스트에서 수행하여 검증 할 수 있다
 * @author dowon
 *
 */
public class WordCountMapperTest {

    @Test
    public void testMap() {
        // 1. 설
        Text value = new Text("Hello World Bye World");

        MapDriver<LongWritable, Text, Text, IntWritable> mapDriver = new MapDriver();
        mapDriver.withMapper(new WordCountMapper());
        mapDriver.withInputValue(value);

        // 2. 검정 및 실행
        // 순서를 정확히 해야 에러없이 수행된다. 빼먹어도 에러가 난다
        mapDriver.withOutput(new Text("Hello"), new IntWritable(1));
        mapDriver.withOutput(new Text("World"), new IntWritable(1));
        mapDriver.withOutput(new Text("Bye"), new IntWritable(1));
        mapDriver.withOutput(new Text("World"), new IntWritable(1));
        mapDriver.runTest();
    }
}
