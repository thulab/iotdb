package cn.edu.tsinghua.iotdb.performance;

public class NumberUtil {
    public int sum(){
        int result = 0;
        for(int i = 0; i< 100; i++){
            result += i * i;
        }
        return result;
    }

    public static void main(String[] args){
        while (true) {
            Thread.currentThread().setName("计算");
            NumberUtil util = new NumberUtil();
            int result = util.sum();
            System.out.println(result);
            try {
                Thread.sleep(5000);
            }catch (InterruptedException e){
            }
        }
    }
}
