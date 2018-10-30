package sql.qlis.Utils.Answer_Collect;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import sql.qlis.Utils.JointUtil;
import sql.qlis.Utils.MD5Util;

import java.io.IOException;

public class AC_MapJoin extends Mapper<LongWritable ,Text,NullWritable,Text> {
  private  static  Text k = new Text();
  protected  void  map(LongWritable key ,Text value ,Context context) throws IOException, InterruptedException {
      String lines  = value.toString();
      String[] words  = lines.split("\u0001");

      String[] arr ={words[1],words[8],words[6],words[5],
              MD5Util.MD5(words[4]),words[10],words[9],words[12],
              words[11],words[11]
      };
      k.set(JointUtil.joint(arr));
      if (words[14].equals("1")){
          context.write(NullWritable.get(),k);
      }

  }





}
