package sql.qlis.Utils.Answer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import sql.qlis.Utils.JointUtil;
import sql.qlis.Utils.MD5Util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * (主表)TemTable-5  key--取字段1（学号）  value--所有字段
 * answer_paper   key取字段6（学号）  value--剩下需要的字段
 * 生成的顺序： --结果顺序  最后一个正确是否得分  师傅等于每题分数的判断
 *
 * */
public class AnswerMapJoin extends Mapper<LongWritable,Text,NullWritable,Text> {
    private  static  Text k  = new Text();

    //answer_paper放在缓存中
    private Map<String,String> answer_paper   =  new ConcurrentHashMap<String,String>();

    @Override
    protected  void  setup(Mapper.Context context ) throws IOException {
        Path[] paths = context.getLocalCacheFiles();
        for (int i = 0; i<paths.length;i++){
            BufferedReader br  = new BufferedReader(new FileReader(paths[i].toString()));
            String str = null ;
            while ((str = br.readLine())!= null){
                String[] splits  = str.split("\u0001");
                answer_paper.put(splits[5],str);

            }
        }
    }
    @Override
    protected   void  map(LongWritable key ,Text value ,Context context) throws IOException, InterruptedException {
    String lines  = value.toString();
    String[] sp  = lines.split("\u0001");
    //  生成顺序:----------结果顺序 ,最后一个正确与否通过得分是否等于每题分数判断
    //个正确与否通过得分是否等于每题分数判断
    String a_p_str  = answer_paper.get(sp[0]);
    String[] a_s_words  = a_p_str.split("\u0001");
    String last_words = " ";
    if (sp[5].equals(sp[6]))
        last_words="1";
    else
        last_words ="0";


    String[] result = {a_s_words[1],a_s_words[8],a_s_words[7],
            sp[0], MD5Util.MD5(a_s_words[4]),
            sp[1],sp[2],sp[3],sp[4],sp[5],sp[6],last_words};


    k.set(JointUtil.joint(result));
    context.write(NullWritable.get(),k);
    }

}
