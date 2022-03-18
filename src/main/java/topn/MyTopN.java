package topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * program argument: /data/fof/input    /data/fof/output
 *
 *  前提
 * 		vi dict.txt
 * 			1	北京
 * 			2	上海
 * 			3	广州
 *
 * 		hdfs dfs -put dict.txt	/data/topn/dict.txt
 */
public class MyTopN {


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration(true);

//        conf.set("mapreduce.framework.name","local");
        conf.set("mapreduce.app-submission.cross-platform","true");


        String[] other = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job  job = Job.getInstance(conf);
        job.setJarByClass(MyTopN.class);

        job.setJobName("TopN");
        // client mapTask reduceTask都是单机运行的话，这行不需要
        job.setJar("I:\\BigDataArchitect\\hdfsdemo\\target\\hdfs-demo-1.0-SNAPSHOT.jar");
        //客户端规划的时候讲join的右表cache到mapTask出现的节点上(map端的join只能集群运行,所以mapreduce.framework.name= yarn);这行要注释掉)
        job.addCacheFile(new Path("/data/topn/dict.txt").toUri());





        //初学者，关注的是client端的代码梳理：因为把这块写明白了，其实你也就真的知道这个作业的开发原理；

        //maptask
        //input

        TextInputFormat.addInputPath(job,new Path(other[0]));

        Path outPath = new Path(other[1]);

        if(outPath.getFileSystem(conf).exists(outPath))  outPath.getFileSystem(conf).delete(outPath,true);
        TextOutputFormat.setOutputPath(job,outPath);


        //key
        //map
        job.setMapperClass(TMapper.class);
        job.setMapOutputKeyClass(TKey.class);
        job.setMapOutputValueClass(IntWritable.class);

        //partitioner  按  年，月  分区  -》  分区 > 分组  按 年分区！！！！！！
            //分区器潜台词：满足  相同的key获得相同的分区号就可以~！
        job.setPartitionerClass(TPartitioner.class);
        //sortComparator  年，月，温度  且  温度倒序
        job.setSortComparatorClass(TSortComparator.class);
        //combine
//        job.setCombinerClass();




        //reducetask
        //groupingComparator
        job.setGroupingComparatorClass(TGroupingComparator.class);
        //reduce
        job.setReducerClass(TReducer.class);

        job.waitForCompletion(true);



    }


}
