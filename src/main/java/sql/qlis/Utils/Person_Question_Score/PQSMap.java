package sql.qlis.Utils.Person_Question_Score;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;
import sql.qlis.Utils.JointUtil;

import java.io.IOException;
import java.util.Iterator;

/**
 *  切分客观题串得到的学号，题目，得分
 *
 *
 *
 *
 * */
public class PQSMap extends Mapper<LongWritable,Text,NullWritable,Text>{
    private  static  Text k = new Text();
    @Override
    protected  void  setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }
    protected  void  map(LongWritable key ,Text value,Context context) throws IOException, InterruptedException {
        String lines  = value.toString();
        String[] words  = lines.split("\u0001");

        JSONObject json = null ;
        try{
             json  = new JSONObject(words[16]);
             //取外层所有的key的Iterator集合
             Iterator keys = json.keys();
             while (keys.hasNext()){
                 //得到每个key
                 String title  = (String) keys.next();
                 k.set(title);

                 //得到外层key（题目），对应的value（答案和得分）
                 String values  = json.get(title).toString();
                 JSONObject answerJSON  = new JSONObject(values);

                 //得到得分
                 String score = answerJSON.get("score").toString();

                 String[] arr = {words[5],title,score};

                 k.set(JointUtil.joint(arr));

                 context.write(NullWritable.get(),k);

             }


        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

}
