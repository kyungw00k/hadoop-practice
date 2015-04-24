package mapreduce.delaycount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Created by humphrey on 15. 4. 24..
 */
public class ArrivalDelayCount {
    public static final String INPUT_PATH = "/dataexpo/2009/input";
    public static final String OUTPUT_PATH = "/dataexpo/2009/output-arrival";

    public static void main(String[] args) throws IOException {

        Configuration globalConf = new Configuration();
        globalConf.set("fs.default.name", "hdfs://master.local:9000");
        globalConf.set("mapred.job.tracker", "hdfs://master.local:9001");

        FileSystem hdfs = FileSystem.get(globalConf);

        Path workingDir = hdfs.getWorkingDirectory();
        Path newFolderPath = new Path(OUTPUT_PATH);
        newFolderPath = Path.mergePaths(workingDir, newFolderPath);

        if (hdfs.exists(newFolderPath)) {
            //Delete existing Directory
            hdfs.delete(newFolderPath, true);
            System.out.println("Existing Folder Deleted.");
        }

        JobConf conf = new JobConf(globalConf);
        conf.setJobName("ArrivalDelayCount");
        conf.setMapperClass(ArrivalDelayCountMapper.class);
        conf.setReducerClass(DelayCountReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(conf, new Path(OUTPUT_PATH));

        JobClient.runJob(conf);
    }
}
