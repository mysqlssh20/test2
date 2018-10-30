package sql.qlis.Utils.TemTable_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;


public class TemTable_2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf  = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadp01:9000/");

        //缓存地址 必须到文件
        Path cachePath_1 = new Path("hdfs://hadp01:9000/qlis/input/question/part-m-00000");
        Path cachePath_2 = new Path("hdfs://hadp01:9000/qlis/input/question/part-m-00001");
        Path cachePath_3 = new Path("hdfs://hadp01:9000/qlis/input/question/part-m-00002");
        Path cachePath_4 = new Path("hdfs://hadp01:9000/qlis/input/question/part-m-00003");

        Job job  = Job.getInstance(conf, "TemTable_2");

        job.setJarByClass(TemTable_2.class);
        job.setMapperClass(TemMapJoin_2.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        job.addCacheFile(cachePath_1.toUri());
        job.addCacheFile(cachePath_2.toUri());
        job.addCacheFile(cachePath_3.toUri());
        job.addCacheFile(cachePath_4.toUri());


        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path("hdfs://hadp1:9000/qlis/tem/TemTable_1"));
        FileOutputFormat.setOutputPath(job
                , new Path("hdfs://hadp1:9000/qlis/tem/TemTable_2"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0:1);
    }
}
