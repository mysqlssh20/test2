package sql.qlis.Utils.TemTable_1;

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
 * TemTable_1:
 *
 exam表
 key --取字段3(试卷模板id)
 value  --字段2(考试名--考试阶段)
 (主表)paper_template_part表
 key --取字段2(试卷模板id)
 value  --字段1(题型),字段4(每题分数)
 生成顺序 : (题目id),exam_name(所属阶段),(题目分数)

 * */
public class TemMapJoin_1 extends Mapper<LongWritable,Text,NullWritable,Text> {
    private static Text k = new Text();

    //paper_template_part放在缓存中
    private Map<String, String> exam = new ConcurrentHashMap<String, String>();

    @Override
    protected void setup(Context context) throws IOException {
        Path[] paths = context.getLocalCacheFiles();
        for (int i = 0; i < paths.length; i++) {
            BufferedReader br = new BufferedReader(new FileReader(paths[i].toString()));
            String str = null;

            while ((str = br.readLine()) != null) {
                String[] splits = str.split("\u0001");
                exam.put(splits[2], splits[1]);
            }
        }
    }
    //mapreduce处理的paper_template_part_question_number表
@Override
    protected  void  map(LongWritable key ,Text value,Context context) throws IOException, InterruptedException {
    String lines  = value.toString();
    String[] sp  = lines.split("\u0001");

    //生成顺序：题目（ID），exam_name(所属阶段)，题目分数
    String[] result = {sp[0],exam.getOrDefault(sp[1],sp[3])};//////////////////////
    k.set(JointUtil.joint(result));
    context.write(NullWritable.get(),k);

}

}



