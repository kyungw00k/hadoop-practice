package mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class WordCount {
    public static final String INPUT_PATH = "/wordcount";
    public static final String OUTPUT_PATH = "/wordcount/output";

    public static void main(String[] args) throws IOException {

        Configuration globalConf = new Configuration();
        globalConf.set("fs.default.name", "hdfs://master.local:9000");
        globalConf.set("mapred.job.tracker", "hdfs://master.local:9001");

        FileSystem hdfs = FileSystem.get(globalConf);

        // 1. Create & Delete Directories
        Path workingDir = hdfs.getWorkingDirectory();
        Path newFolderPath = new Path(INPUT_PATH);
        newFolderPath = Path.mergePaths(workingDir, newFolderPath);

        if (hdfs.exists(newFolderPath)) {
            //Delete existing Directory
            hdfs.delete(newFolderPath, true);
            System.out.println("Existing Folder Deleted.");
        }

        // 2. Create new Directory
        hdfs.mkdirs(newFolderPath);
        System.out.println("Folder Created.");

        // 3. Copying File from local to HDFS
        Path localFilePath = new Path("src/main/resources/text");
        Path hdfsFilePath = new Path(INPUT_PATH + "/text");
        hdfs.copyFromLocalFile(localFilePath, hdfsFilePath);

        if (hdfs.exists(hdfsFilePath)) {
            System.out.println("Test File Uploaded.");
        }

        // 4. configuration Mapper & Reducer of Hadoop
        JobConf conf = new JobConf(globalConf);
        conf.setJobName("wordcount");
        conf.setMapperClass(WordCountMapper.class);
        conf.setReducerClass(WordCountReducer.class);

        // 5. final output key type & value type
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        // 6. in/output format
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        // 7. set the path of file for read files
        FileInputFormat.setInputPaths(conf, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(conf, new Path(OUTPUT_PATH));

        // 5. run job
        JobClient.runJob(conf);
    }

}