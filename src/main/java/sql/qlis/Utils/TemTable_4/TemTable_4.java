package sql.qlis.Utils.TemTable_4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TemTable_4 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf  = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadp1:9000/");

        Path cachePath_1 = new Path("hdfs://hadp1:9000/qlis/input/question_type/part-m-00000");
        Path cachePath_2 = new Path("hdfs://hadp1:9000/qlis/input/question_type/part-m-00001");
        Path cachePath_3 = new Path("hdfs://hadp1:9000/qlis/input/question_type/part-m-00003");
        Path cachePath_4 = new Path("hdfs://hadp1:9000/qlis/input/question_type/part-m-00004");

        Job job = Job.getInstance(conf, "TemTable_4");
        //指定程序的jar包运行的本地路径
        job.setJarByClass(TemTable_4.class);
        //本业务需要执行的Map类
        job.setMapperClass(TemMapJoin_4.class);

        //输出的Key
        job.setMapOutputKeyClass(NullWritable.class);
        //输出的value
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        //缓存表添加到缓存
        job.addCacheFile(cachePath_1.toUri());
        job.addCacheFile(cachePath_2.toUri());
        job.addCacheFile(cachePath_3.toUri());
        job.addCacheFile(cachePath_4.toUri());

        //设置job的输出数据的类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path("hdfs://hadp1:9000/qlis/input/question"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://hadp1:9000/qlis/tem/TemTable_4"));


        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
