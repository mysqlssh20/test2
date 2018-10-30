package com.qianfeng.qlis.UDFHive;

///**
// *1、	输入年月日输出其生日和星座
// 数据:
// 1991-10-23
// 1990-02-01
// 1992-08-18
// 1994-06-13
// 1992-04-07
// 1996-01-19
// 1994-12-29
// 1995-01-03
// 1993-09-16
// 1999-11-02
// 存储到一个表中通过UDF处理数据将结果数据写到一个新的表中
// 输入效果:
// 1991-10-23	天秤座	27
//
// * */
//
//import org.apache.hadoop.hive.ql.exec.Description;
//import org.apache.hadoop.hive.ql.exec.UDF;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.joda.time.DateTime;
//import org.joda.time.format.DateTimeFormat;
//import org.joda.time.format.DateTimeFormatter;
//import java.util.Date;
//
//
//public class UDFXingZuoSignCn extends UDF {
//    @Description(name = "XingZuo_cn"        ,
//            value = "_FUNC_(date) - from the input date string or separate month and day arguments, returns the sing of the XingZuo."        , extended = "Example:\n > select _FUNC_(date_string) from src;\n > select _FUNC_(month, day) from src;")public class UDFZodiacSignCn extends UDF {
//    public final static DateTimeFormatter DEFAULT_DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
//    private Text result = new Text();
//    public UDFXingZuoSignCn() {    }
//    public Text evaluate(Text birthday) {
//      DateTime dateTime = null;
// try {
// dateTime = DateTime.parse(birthday.toString(),
// DEFAULT_DATE_FORMATTER);        }
// catch (Exception e)
// {            return null;        }
//   return evaluate(dateTime.toDate());
//  }
// public Text evaluate(Date birthday) {
//    DateTime dateTime = new DateTime(birthday);
//    return evaluate(new IntWritable(dateTime.getMonthOfYear()),
//            new IntWritable(dateTime.getDayOfMonth()));    }
//    public Text evaluate(IntWritable month, IntWritable day) {
//     result.set(getXingzuo(month.get(), day.get()));
//     return result;    }
//
//
//     private String getXingzuo(int month, int day) {
//     String[] Array = {"白羊座", "金牛座", "双子座", "巨蟹座", "狮子座", "处女座", "天秤座", "天蝎座", "射手座", "摩羯座", "水瓶座", "双鱼座"};
//     int[]    splitDay = {19, 18, 20, 20, 20, 21, 22, 22, 22, 22, 21, 21};
//            int index = month;
//              if (day <= splitDay[month - 1]) {
//                  index = index - 1;
//              } else if (month == 12) {
//                  index = 0;
//              }
//                  return Array[index];
//
//    }
//          public static void main(String[] args) {
//          	UDFXingZuoSignCn udfXingZuoSignCn = new UDFXingZuoSignCn();
//          	System.out.println("1991-10-23:     "+ udfXingZuoSignCn.evaluate());
//
//              }
// }
