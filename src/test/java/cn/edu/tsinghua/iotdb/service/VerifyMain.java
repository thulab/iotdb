package cn.edu.tsinghua.iotdb.service;

/**
 * Created by beyyes on 17/12/3.
 */
public class VerifyMain {

    private static String[] stringValue = new String[]{"A", "B", "C", "D", "E"};

    public static void main(String[] args) {

        int cnt = 0;

        // insert large amount of data    time range : 3000 ~ 13600
        for (int time = 3000; time < 13600; time++) {

            if (time % 100 >= 20)
                cnt++;

            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 100);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 17);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 22);
            sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, stringValue[time % 5]);
        }


        // insert large amount of data    time range : 13700 ~ 24000
        for (int time = 13700; time < 24000; time++) {
            if (time % 70 >= 20)
                cnt++;

            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 70);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 40);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 123);
        }

        System.out.println("!!!!" + cnt);

        // buffwrite data, unsealed file
        for (int time = 100000; time < 101000; time++) {
            if (time % 20 >= 20)
                cnt++;
            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 20);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 30);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 77);
        }


        // bufferwrite data, memory data
        for (int time = 200000; time < 201000; time++) {

            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, -time % 20);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, -time % 30);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, -time % 77);
        }

        // overflow delete
        // =====  statement.execute("DELETE FROM root.vehicle.d0.s1 WHERE time < 3200");

        // overflow insert, time < 3000
        for (int time = 2000; time < 2500; time++) {
            if (time  >= 20)
                cnt++;
            String sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time);
            sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time + 1);
            sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time + 2);
            sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, stringValue[time % 5]);
        }

        System.out.println("====" + cnt);  // 16340

        // overflow update
        // statement.execute("UPDATE root.vehicle SET d0.s1 = 11111111 WHERE time > 23000 and time < 100100");

    }
}
