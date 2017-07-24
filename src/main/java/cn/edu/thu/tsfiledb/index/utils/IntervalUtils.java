package cn.edu.thu.tsfiledb.index.utils;

import cn.edu.thu.tsfile.common.utils.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author Jiaye Wu
 */
public class IntervalUtils {

    public static List<Pair<Long, Long>> sortAndUnion(List<Pair<Long, Long>> intervals1, List<Pair<Long, Long>> intervals2) {
        intervals1.sort(Comparator.comparing(o -> o.left));
        intervals2.sort(Comparator.comparing(o -> o.left));
        return union(intervals1, intervals2);
    }

    public static List<Pair<Long,Long>> union(List<Pair<Long, Long>> intervals1, List<Pair<Long, Long>> intervals2) {
        List<Pair<Long, Long>> ret = new ArrayList<>();

        int index1 = 0, index2 = 0;
        Pair<Long, Long> last1 = null, last2 = null;
        while (index1 < intervals1.size() && index2 < intervals2.size()) {
            Pair<Long, Long> position1 = intervals1.get(index1);
            Pair<Long, Long> position2 = intervals2.get(index2);
            if (last1 == null) last1 = new Pair<>(position1.left, position1.right);
            if (last2 == null) last2 = new Pair<>(position2.left, position2.right);

            if (last1.right + 1 < last2.left) {
                ret.add(last1);
                index1++;
                last1 = null;
            } else if (last2.right + 1 < last1.left) {
                ret.add(last2);
                index2++;
                last2 = null;
            } else {
                if (last1.right < last2.right) {
                    if (last1.left < last2.left) {
                        last2.left = last1.left;
                    }
                    index1++;
                    last1 = null;
                } else {
                    if (last2.left < last1.left) {
                        last1.left = last2.left;
                    }
                    index2++;
                    last2 = null;
                }
            }
        }
        for (int i = index1; i < intervals1.size(); i++) {
            Pair<Long, Long> position1 = intervals1.get(i);
            if (last1 == null) last1 = new Pair<>(position1.left, position1.right);
            ret.add(last1);
            last1 = null;
        }
        for (int i = index2; i < intervals2.size(); i++) {
            Pair<Long, Long> position2 = intervals2.get(i);
            if (last2 == null) last2 = new Pair<>(position2.left, position2.right);
            ret.add(last2);
            last2 = null;
        }

        return ret;
    }
}
