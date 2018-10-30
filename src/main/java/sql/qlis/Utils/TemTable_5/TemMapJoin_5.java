package sql.qlis.Utils.TemTable_5;


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

/**
 * (主表)TemTable-3 	 key--取字段2(题目id) value--所有字段
 * TemTable_4		key--取字段1(题目id) value--字段2(主客观)
 * 生成顺序:(学号),(题目id),exam_name(所属阶段),(题目难度),(主客观),(每题分数),(每题得分)
 */
public class TemMapJoin_5  extends Mapper<LongWritable,Text,NullWritable,Text>{
    private  static  Text k = new Text();

    //TemTable_4放在缓存中
    private Map<String,String> TemTable_5 = new ConcurrentHashMap<String, String>();

    @Override
    protected  void  setup(Mapper.Context context) throws IOException {
        Path[] paths  = context.getLocalCacheFiles();
        for(int i = 0;i<paths.length;i++){
            BufferedReader br  = new BufferedReader(new FileReader(paths[i].toString()));
            String str = null;
            while ((str = br.readLine())!=null){
                String[] splits  = str.split("\u0001");
                TemTable_5.put(splits[0],splits[1]);
            }
        }
    }
@Override
    protected  void  map(LongWritable key ,Text value,Context context) throws IOException, InterruptedException {
    String lines  = value.toString();
    String[] sp  = lines.split("\u0001");
    //todo (学号),(题目id),exam_name(所属阶段),(题目难度),(主客观),(每题分数),(每题得分)
    String[] result = {sp[0],sp[1],sp[2],sp[3],TemTable_5.get(sp[1]),sp[4],sp[5]};
    k.set(JointUtil.joint(result));
    context.write(NullWritable.get(),k);
    }


}
