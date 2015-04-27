package mapreduce.delaycount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class DelayCountReducerWithMultipleOutputs extends Reducer<Text, IntWritable, Text, IntWritable> {

    private MultipleOutputs<Text, IntWritable> mos;

    // reduce 출력키
    private Text outputKey = new Text();

    // reduce 출력값
    private IntWritable result = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<Text, IntWritable>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String[] columns = key.toString().split(",");

        outputKey.set(columns[1] + "," + columns[2]);

        if (columns[0].equals("D")) {
            int sum = 0;

            for ( IntWritable value : values ) {
                sum += value.get();
            }

            result.set(sum);

            mos.write("departure", outputKey, result);
        } else {
            int sum = 0;

            for ( IntWritable value : values ) {
                sum += value.get();
            }

            result.set(sum);

            mos.write("arrival", outputKey, result);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
