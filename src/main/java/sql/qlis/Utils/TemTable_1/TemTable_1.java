package sql.qlis.Utils.TemTable_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.net.URI;

/**
 * paper_template_part_question_number表
 * 和
 * paper_template_part表join
 * 取数据
 * paper_template_part_id //试题模板组成表
 * paper_template_id///试卷模板ID
 * question_type_id//试题类型ID
 * per_questio_mark//每题分数
 */
public class TemTable_1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf  = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadp01:9000/");
     //   conf.set("mapreduce.framework.name","local");
        Path cachePath_1= new Path("hdfs://hadp01:9000/qlis/input/excam/part-m-00000");
        Path cachePath_2= new Path("hdfs://hadp01:9000/qlis/input/excam/part-m-00001");
        Path cachePath_3= new Path("hdfs://hadp01:9000/qlis/input/excam/part-m-00002");
        Path cachePath_4= new Path("hdfs://hadp01:9000/qlis/input/excam/part-m-00003");

        Job job  = Job.getInstance(conf,"TemTable_1");
        job.setJarByClass(TemTable_1.class);
        job.setMapperClass(TemMapJoin_1.class);

        //输出的key  输出的value
        job.setOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        //缓存表添加到缓存
        job.addCacheFile(cachePath_1.toUri());
        job.addCacheFile(cachePath_2.toUri());
        job.addCacheFile(cachePath_3.toUri());
        job.addCacheFile(cachePath_4.toUri());

        //设置job的输出数据的类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path("hdfs://hadp01:9000/qlis/input/paper_template_part"));
        //输出路径
        //FileOutputFormat.setOutputPath(job,new Path("hdfs://hadoop1:9000/qlis/tem/TemTable_1"));
        FileOutputFormat.setOutputPath(job ,new Path("hdfs://hadp01:9000/qlis/tem/TemTable_1"));
        boolean b = job.waitForCompletion(true);
        System.exit(b? 0:1);


    }
}
