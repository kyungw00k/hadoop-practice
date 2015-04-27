package mapreduce.delaycount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DelayCountWithCounter extends Configured implements Tool {
    public static final String INPUT_PATH = "/dataexpo/2009/input";
    public static final String OUTPUT_PATH = "/dataexpo/2009/output-counter";

    public static void main(String[] args) throws Exception {
        Configuration globalConf = new Configuration();
        globalConf.set("fs.default.name", "hdfs://master.local:9000");
        globalConf.set("mapred.job.tracker", "master.local:9001");
        globalConf.set("mapreduce.framework.name", "yarn");
        globalConf.set("yarn.resourcemanager.address", "master.local:8032");

        ToolRunner.run(globalConf, new DelayCountWithCounter(), args);
    }

    @Override
    public int run(String[] args) throws Exception {

        FileSystem hdfs = FileSystem.get(getConf());

        Path workingDir = hdfs.getWorkingDirectory();
        Path newFolderPath = new Path(OUTPUT_PATH);
        newFolderPath = Path.mergePaths(workingDir, newFolderPath);

        if (hdfs.exists(newFolderPath)) {
            //Delete existing Directory
            hdfs.delete(newFolderPath, true);
            System.out.println("Existing Folder Deleted.");
        }

        Job conf = Job.getInstance(getConf(), "DelayCountWithCounter");

        conf.setUser("vagrant");

        conf.setJarByClass(getClass());

        conf.setMapperClass(DelayCountMapperWithCounter.class);
        conf.setReducerClass(DelayCountReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setInputFormatClass(TextInputFormat.class);
        conf.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(conf, new Path(OUTPUT_PATH));

        return conf.waitForCompletion(true) ? 0 : 1;
    }
}
