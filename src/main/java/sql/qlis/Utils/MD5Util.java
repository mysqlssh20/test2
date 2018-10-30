package sql.qlis.Utils;

import java.security.MessageDigest;

/**
 * MD5加密
 *
 *
 * */


public class MD5Util {
    public  static  String MD5(String s){
        try{
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] bytes = md5.digest(s.getBytes("utf-8"));
            return  toHex(bytes);
        }catch (Exception e) {
           throw  new  RuntimeException(e);
        }


    }
    private  static  String  toHex(byte[] bytes) {
    final  char[]  HEX_DIGITS ="0123456789ABCDEF".toCharArray();
        StringBuffer ret  = new StringBuffer(bytes.length * 2);
        for (byte aByte:bytes){
            ret.append(HEX_DIGITS[(aByte >> 4) & 0x0f]);
            ret.append(HEX_DIGITS[aByte & 0x0f]);
        }
         return  ret.toString();
    }
}
