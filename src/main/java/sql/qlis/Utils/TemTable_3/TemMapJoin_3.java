package sql.qlis.Utils.TemTable_3;


import org.apache.commons.lang.ObjectUtils;
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
 * TemTable_3 ------>
 * (主表)perpson_question_score 	key--取字段2(题目id) value --剩下所有字段
 * TemTable_2 		key--取字段1(题目id) value --剩下所有字段
 * 生成顺序:(学号),(题目id),exam_name(所属阶段),(题目难度),(每题分数),(每题得分)
 */
public class TemMapJoin_3 extends Mapper<LongWritable ,Text,NullWritable,Text>{
private  static  Text k = new Text();

   //paper_template_part放在缓存中
   private Map<String ,String> TemTable_2=new ConcurrentHashMap<String,String>();
   @Override
    protected  void  setup(Context context) throws IOException {
       Path[] paths  = context.getLocalCacheFiles();
       for (int i =0 ; i<paths.length;i++){
           BufferedReader br  = new BufferedReader(new FileReader(paths[i].toString()));
           String str =null ;
           while ((str = br.readLine()) !=null){
               String[] splits  = str.split("\u0001");
               TemTable_2.put(splits[0],str);
           }
       }
   }

   @Override
    protected  void  map(LongWritable key ,Text value ,Context context) throws IOException, InterruptedException {
       String lines  = value.toString();
       String[] sp  = lines.split("\u0001");

       //生成顺序:(学号),(题目id),exam_name(所属阶段),(题目难度),(每题分数),(每题得分)
       if (TemTable_2.get(sp[1]) != null){
           String[] result = {sp[0], TemTable_2.get(sp[1]), sp[2]};
           k.set(JointUtil.joint(result));
           context.write(NullWritable.get(), k);
       }

   }
}
