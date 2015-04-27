package mapreduce.delaycount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DelayCountMapperWithCounter extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable outputValue = new IntWritable(1);
    private String workType;
    private Text outputKey = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        workType = context.getConfiguration().get("workType");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() > 0) {
            String[] columns = value.toString().split(",");

            if (columns != null && columns.length > 0) {
                try {
                    outputKey.set(columns[0] + "," + columns[1]);

                    if (workType.equals("departure")) {
                        if (!columns[15].equals("NA")) {
                            int depDelayTime = Integer.parseInt(columns[15]);

                            if (depDelayTime > 0) {
                                context.write(outputKey, outputValue);
                            } else if (depDelayTime == 0) {
                                context.getCounter(DelayCounters.scheduled_departure).increment(1);
                            } else if (depDelayTime < 0) {
                                context.getCounter(DelayCounters.early_departure).increment(1);
                            }
                        } else {
                            context.getCounter(DelayCounters.not_available_departure).increment(1);
                        }

                    } else if (workType.equals("arrival")) {
                        if (!columns[14].equals("NA")) {
                            int arrDelayTime = Integer.parseInt(columns[14]);

                            if (arrDelayTime > 0) {
                                context.write(outputKey, outputValue);
                            } else if (arrDelayTime == 0) {
                                context.getCounter(DelayCounters.scheduled_arrival).increment(1);
                            } else if (arrDelayTime < 0) {
                                context.getCounter(DelayCounters.early_arrival).increment(1);
                            }

                        } else {
                            context.getCounter(DelayCounters.not_available_arrival).increment(1);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
