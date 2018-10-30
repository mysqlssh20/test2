package sql.qlis.Utils.TemTable_4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import sql.qlis.Utils.JointUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * TemTable_4 ------>
 * (主表)question  		key--字段3(试题类型) value --取字段1(题目id)
 * question_type 		key--字段1(试题类型) value --字段4(主客观)
 * 生成顺序: (题目id),(主客观)
 */
public class TemMapJoin_4 extends Mapper<LongWritable,Text,NullWritable,Text> {
private  static  Text k = new Text();

    //question_type放在缓存当中
    private Map<String,String> question_type = new ConcurrentHashMap<String, String>();

    @Override
    protected  void  setup(Context context) throws IOException {
        Path[] paths  = context.getLocalCacheFiles();
        for (int i = 0;i<paths.length;i++){
            BufferedReader br  = new BufferedReader(new FileReader(paths[i].toString()));
            String str = null ;
            while ((str = br.readLine())!= null){
                String[] splits  = str.split("\u0001");
                question_type.put(splits[0],splits[3]);
            }
        }
    }

    @Override
    protected  void map(LongWritable key ,Text value ,Context context) throws IOException, InterruptedException {
        String lines  = value.toString();
        String[] sp  = lines.split("\u0001");

        //生成顺序：题目（ID）  主客观
        Pattern parrtern  = Pattern.compile("[0-9]*");
        boolean b = parrtern.matcher(sp[0]).matches();
        if (b){
            String[] result ={sp[0],question_type.get(sp[2])};
            k.set(JointUtil.joint(result));
            context.write(NullWritable.get(),k);
        }
    }

}
