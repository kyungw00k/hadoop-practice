package mapreduce.delaycount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class ArrivalDelayCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable outputValue = new IntWritable(1);

    private Text outputKey = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        if (key.get() > 0) {
            String[] columns = value.toString().split(",");

            if (columns != null && columns.length > 0 ){
                try {
                    outputKey.set(columns[0] + "," + columns[1]);

                    if ( !columns[14].equals("NA") ) {
                        int depDelayTime = Integer.parseInt(columns[14]);

                        if ( depDelayTime > 0 ) {
                            output.collect(outputKey, outputValue);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
