package cn.edu.tsinghua.postback.iotdb.test;
/**
 * Created by stefanie on 26/07/2017.
 */
import java.util.Random;
import java.time.LocalDate;

public class RandomNum {

    static Random random = new Random();

    public static long getRandomLong(long min, long max) {
        return Math.abs(random.nextLong()) % (max-min+1) + min;
    }

    public static int getRandomInt(int min, int max) {
        //return Math.abs(random.nextInt() % (max-min+1) + min);
        return (random.nextInt(10000) % (max-min) + min);
    }

    public static float getRandomFloat(float min, float max) {

        Random random = new Random();
        return (random.nextFloat() * (max-min) + min);
    }

    public static int getAbnormalData(int frequency) {
        Random random = new Random();
        return (Math.abs(random.nextInt()) % frequency);
    }

    public static String getRandomText(int length) {

        String base = "abcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        String st = "";
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(base.length());
            st += base.charAt(number);
        }
        return st;

    }

    public static void main(String[] argc) throws Exception {

//        for (int i = 0; i < 10; i++) {
//            //System.out.println(r.getRandomLong(1,100));
//            System.out.println(RandomNum.getRandomDouble(1,5));
//        }
//        System.out.println(System.currentTimeMillis());
//        for (int i = 0; i < 1000; i++) {
//            System.out.println(getAbnormalData(10));
//            Thread.sleep(1000);
//        }
        for (int i = 0; i < 10; i++) {
            System.out.println(getRandomText(10));
        }
    }

}