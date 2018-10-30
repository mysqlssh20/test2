package sql.qlis.Utils.TemTable_2;


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
 * TemTable_2 ----->
 * question 	key --取字段1(题目id) value --字段4(题目难度)
 * (主表)TemTable_1 	key--取字段1(题目id) value --剩下所有字段
 * 生成顺序:(题目id),exam_name(所属阶段),(题目难度),(题目分数)
 */
public class TemMapJoin_2 extends Mapper<LongWritable,Text,NullWritable,Text> {
    private  static  Text k = new Text();

    //question放在缓存中
    private Map<String ,String> question = new ConcurrentHashMap<String, String>();
    @Override
    protected  void  setup(Context context) throws IOException {
        Path[] paths  = context.getLocalCacheFiles();
        for(int i =0 ;i<paths.length;i++){
            BufferedReader br  = new BufferedReader(new FileReader(paths[i].toString()));
            String str = null;
            while ((str = br.readLine()) != null){
                String[] splits = str.split("\u0001");

                Pattern pattern = Pattern.compile("[0-9]*");
                boolean b = pattern.matcher(splits[0]).matches();
                if (b){
                    question.put(splits[0],splits[3]);
                }
            }
        }
    }
    @Override
    protected  void  map(LongWritable key ,Text value ,Context context) throws IOException, InterruptedException {
        String lines  = value.toString();
        String[] sp  = lines.split("\u0001");

        //生成顺序：（题目ID） exam_name（所属阶段），（题目难度），（题目分数）

        String[] result = {sp[0],sp[1],question.get(sp[0]),sp[2]};
        k.set(JointUtil.joint(result));
        context.write(NullWritable.get(),k);

    }



}
