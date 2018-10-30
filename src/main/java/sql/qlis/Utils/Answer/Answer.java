package sql.qlis.Utils.Answer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class Answer {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf  = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadp01:9000/");
        conf.set("mapreduce.framework.name","local");
        //缓存地址 必须具体到文件，不能只文件夹
        Path cachePath_1 = new Path("hdfs://hadp01:9000/qlis/input/answer_paper/part-m-00000");
        Path cachePath_2 = new Path("hdfs://hadp01:9000/qlis/input/answer_paper/part-m-00001");
        Path cachePath_3 = new Path("hdfs://hadp01:9000/qlis/input/answer_paper/part-m-00002");
        Path cachePath_4 = new Path("hdfs://hadp01:9000/qlis/input/answer_paper/part-m-00003");

        Job job  = Job.getInstance(conf, "Answer");

        //指定程序的jar包运行的本地路径
        job.setJarByClass(Answer.class);

        //本业务需要执行的Map类
        job.setMapperClass(AnswerMapJoin.class);

        //输出的key
        job.setMapOutputKeyClass(NullWritable.class);

        //输出的value
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);


        //将缓存表添加到缓存
        job.addCacheFile(cachePath_1.toUri());
        job.addCacheFile(cachePath_2.toUri());
        job.addCacheFile(cachePath_3.toUri());
        job.addCacheFile(cachePath_4.toUri());


        //设置job的输出数据的类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.setInputPaths(job,new Path("hdfs://hadp01:9000/qlis/output/answer"));

        FileOutputFormat.setOutputPath(job,new Path("hdfs://hadp01:9000/qlis/output/answer"));


        boolean b = job.waitForCompletion(true);

         System.exit(b ? 0:1);

    }
}
