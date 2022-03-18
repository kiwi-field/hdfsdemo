package wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * @Author kiwi
 * @Date 2022/2/14 20:51
 * @Description 自己写的
 */
public class WordCountDxy {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration(true);


//        conf.set("fs.defaultFS", "hdfs://node01:9000");
        // //让框架知道是windows异构平台运行
        conf.set("mapreduce.app-submission.cross-platform","true");
        Job job = Job.getInstance(conf);

        job.setJar("I:\\BigDataArchitect\\hdfsdemo\\target\\hdfs-demo-1.0-SNAPSHOT.jar");

        job.setJarByClass(WordCountDxy.class);

        job.setJobName("dengxiaoyu");

        Path infile = new Path("/data/wc/input");
        TextInputFormat.addInputPath(job, infile);

        Path outfile = new Path("/data/wc/output");
        if (outfile.getFileSystem(conf).exists(outfile)) outfile.getFileSystem(conf).delete(outfile, true);
        TextOutputFormat.setOutputPath(job, outfile);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);

//        job.setNumReduceTasks(2);
        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }
}
