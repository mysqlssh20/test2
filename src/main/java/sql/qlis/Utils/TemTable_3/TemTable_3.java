package sql.qlis.Utils.TemTable_3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TemTable_3 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf  = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadp1:9000/");
        Path cachePath_1 = new Path("hdfs://hadp1:9000/qlis/tem/TemTable_2/part-m-00000");
        Path cachePath_2 = new Path("hdfs://hadp1:9000/qlis/tem/TemTable_2/part-m-00001");
        Path cachePath_3=new Path("hdfs://hadp1:9000/qlis/tem/TemTable_2/part-m-00002");
        Path cachePath_4 = new Path("hdfs://hadp1:9000/qlis/tem/TemTable_2/part-m-00003");

        Job job  = Job.getInstance(conf, "TemTable_3");

        job.setJarByClass(TemTable_3.class);
        job.setMapperClass(TemMapJoin_3.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        job.addCacheFile(cachePath_1.toUri());
        job.addCacheFile(cachePath_2.toUri());
        job.addCacheFile(cachePath_3.toUri());
        job.addCacheFile(cachePath_4.toUri());

        FileInputFormat.setInputPaths(job,new Path("hdfs://hadp1:9000/qlis/tem/person_question_score"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://hadp1:9000/qlis/tem/TemTable_3"));


        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0:1);


    }
}
