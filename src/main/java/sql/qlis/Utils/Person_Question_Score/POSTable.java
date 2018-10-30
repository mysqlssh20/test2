package sql.qlis.Utils.Person_Question_Score;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class POSTable {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf  = new Configuration();
        conf.set("fs.defaultFS","file:///");
        conf.set("mapreduce.framework.name","local");

        Job job = Job.getInstance(conf, "POSTable");

        //指定程序的jar运行的本地路径
        job.setJarByClass(POSTable.class);
        //本业务需要执行的map类
        job.setMapperClass(PQSMap.class);

        //输出的key
        job.setMapOutputKeyClass(NullWritable.class);
        //输出value
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        //文件来源
        //设置job的输出数据的类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path("hdfs://hadp01:9000/qlis/input/answer_paper"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://hadp01:9000/qlis/tem/person_question_score"));


        boolean b = job.waitForCompletion(true);

        System.exit(b? 0:1);
    }
}
