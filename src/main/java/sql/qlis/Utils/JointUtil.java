package sql.qlis.Utils;

import  org.apache.hadoop.io.Text;

public class JointUtil {
    public  static Text joint(String[] strings){
        StringBuilder sb  = new StringBuilder();
        for (int i =0; i < strings.length;i++){
            if(i == strings.length-1){
                sb.append(strings[i]);
            }else
                sb.append(strings[i]).append("\001");
        }
       return  new Text(sb.toString());

       }

    }




