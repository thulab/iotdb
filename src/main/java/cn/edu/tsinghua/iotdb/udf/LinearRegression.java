package cn.edu.tsinghua.iotdb.udf;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class LinearRegression extends AbstractUDSF {
    private float sumX = 0;
    private float sumY = 0;
    private float sumXX = 0;
    private float sumXY = 0;
    private float sumYY = 0;

    private List<Float> listX;
    private List<Float> listY;

    private float b = 0;
    private float w = 0;
    private int pn;
    private boolean coefsValid;

    private double error;

    public LinearRegression(double error) {
        pn = 0;
        listX = new ArrayList<>();
        listY = new ArrayList<>();

        this.error = error;
    }

    public float getB() {
        validateCoefficients();
        return b;
    }

    public float getW() {
        validateCoefficients();
        return w;
    }

    @Override
    public boolean isCuttingpoint(long time, Comparable<?> value) {
        float y = 0f;
        if (value instanceof Number) {
            y = ((Number) value).floatValue();
        }
        addDatapoint(time, y);

        if (pn == 1) {
            return false;
        }

        if (getR() >= error) {
            reset();
            return true;
        }

        return false;
    }

    private void addDatapoint(long x, float y) {
        sumX += x;
        sumY += y;
        sumXX += x * x;
        sumXY += x * y;
        sumYY += y * y;

        listX.add((float) x);
        listY.add(y);

        ++pn;
        coefsValid = false;
    }


    public float at(float x) {
        if (pn < 2)
            return Float.NaN;
        validateCoefficients();
        return b + w * x;
    }

    public void reset() {
        pn = 0;
        sumX = sumY = sumXX = sumXY = sumYY = 0;
        b = w = 0;
        listX.clear();
        listY.clear();
        coefsValid = false;
    }

    private void validateCoefficients() {
        if (coefsValid)
            return;
        if (pn >= 2) {
            float xBar = sumX / pn;
            float yBar = sumY / pn;
            w = ((pn * sumXY - sumX * sumY) / (pn * sumXX - sumX * sumX));
            Float fw = (Float)w;
            if (fw.isNaN() || fw.isInfinite()) {
                w = 0;
            }
            b = (yBar - w * xBar);
        } else {
            b = w = Float.NaN;
        }
        coefsValid = true;
    }

    public double getR() {
        float sumDeltaY2 = 0;
        float sst = 0;

        for (int i = 0; i < pn; i++) {
            float Yi = listY.get(i);
            float Y = at(listX.get(i));
            float deltaY = Yi - Y;
            float deltaY2 = deltaY * deltaY;
            sumDeltaY2 += deltaY2;
        }

        sst = sumYY - (sumY * sumY) / pn;

        float E = sumDeltaY2 / sst;

        if (sst == 0) {
            return 0;
        }
        return round(E, 4);
    }

    public double round(double v, int scale) {
        BigDecimal b = new BigDecimal(Double.toString(v));
        BigDecimal one = new BigDecimal("1");
        return b.divide(one, scale, BigDecimal.ROUND_HALF_UP).floatValue();
    }

    public static void main(String[] args) {
        LinearRegression line = new LinearRegression(0.2);

        Random random = new Random();
        long s = 0;
        long time = -5;
        float value = 10;
        int figure = 400 + random.nextInt(200);
        int count = 0;
        for (long i = 0; i < 100000000; i++) {
            time = time + 5;
            value = 2.72f;

            ++count;
            if (count >= figure - 100 && count <= figure + 100) {
                value = figure * 10;
                if (count == figure + 100) {
                    count = 0;
                    figure = 400 + random.nextInt(200);
                }
            }

            if (line.isCuttingpoint(time, value)) {
                ++s;
            }
        }

        System.out.println(s);
    }
}
