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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.security.auth.callback.TextOutputCallback;

public class DelayCountWithMultipleOutputs extends Configured implements Tool {
    public static final String INPUT_PATH = "/dataexpo/2009/input";

    public static void main(String[] args) throws Exception {
        Configuration globalConf = new Configuration();
        globalConf.set("fs.default.name", "hdfs://master.local:9000");
        globalConf.set("mapred.job.tracker", "master.local:9001");
        globalConf.set("mapreduce.framework.name", "yarn");
        globalConf.set("yarn.resourcemanager.address", "master.local:8032");

        ToolRunner.run(globalConf, new DelayCountWithMultipleOutputs(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

        if ( otherArgs.length != 1 ) {
            System.err.println("Usage: DelayCountWithMultipleOutputs <out>");
            System.exit(2);
        }

        FileSystem hdfs = FileSystem.get(getConf());

        Path workingDir = hdfs.getWorkingDirectory();
        Path newFolderPath = new Path(otherArgs[0]);
        newFolderPath = Path.mergePaths(workingDir, newFolderPath);

        if (hdfs.exists(newFolderPath)) {
            //Delete existing Directory
            hdfs.delete(newFolderPath, true);
            System.out.println("Existing Folder Deleted.");
        }

        Job conf = Job.getInstance(getConf(), "DelayCountWithMultipleOutputs");

        conf.setUser("vagrant");

        conf.setJarByClass(getClass());

        conf.setMapperClass(DelayCountMapperWithMultipleOutputs.class);
        conf.setReducerClass(DelayCountReducerWithMultipleOutputs.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setInputFormatClass(TextInputFormat.class);
        conf.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(conf, new Path(otherArgs[0]));

        MultipleOutputs.addNamedOutput(conf, "departure", TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(conf, "arrival", TextOutputFormat.class, Text.class, IntWritable.class);

        return conf.waitForCompletion(true) ? 0 : 1;
    }
}
