package io.github.gabrielmmoraes1999.seadb;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Window values for one SELECT list. The frame is the partition, or the rows up to the current one when ordered.
 */
final class Windows {

    private Windows() {
    }

    static boolean ordered(WindowExpr window) {
        return window.orderBy() != null && !window.orderBy().isEmpty();
    }

    static void require(WindowExpr window) {
        String name = window.call().name().toUpperCase(java.util.Locale.ROOT);
        boolean rank = name.equals("ROW_NUMBER") || name.equals("RANK") || name.equals("DENSE_RANK")
                || name.equals("LAG") || name.equals("LEAD");
        if (rank && !ordered(window)) {
            throw new SeaException(name + " requires ORDER BY");
        }
    }

    static Object[] values(WindowExpr window, List<Object[]> partitionKeys, List<Object[]> orderKeys, List<Object> args) {
        require(window);
        int count = args.size();
        Object[] out = new Object[count];
        boolean[] placed = new boolean[count];
        for (int start = 0; start < count; start++) {
            if (placed[start]) {
                continue;
            }
            List<Integer> group = new ArrayList<>();
            for (int i = start; i < count; i++) {
                if (!placed[i] && same(partitionKeys.get(start), partitionKeys.get(i))) {
                    group.add(i);
                    placed[i] = true;
                }
            }
            group.sort((left, right) -> {
                int cmp = compareOrder(orderKeys.get(left), orderKeys.get(right), window.orderBy());
                if (cmp != 0) {
                    return cmp;
                }
                return Integer.compare(left, right);
            });
            fill(window, group, orderKeys, args, out);
        }
        return out;
    }

    private static void fill(WindowExpr window, List<Integer> group, List<Object[]> orderKeys, List<Object> args, Object[] out) {
        String name = window.call().name().toUpperCase(java.util.Locale.ROOT);
        boolean running = window.frame() || ordered(window);
        if (!ordered(window) && !window.frame()) {
            running = false;
        }
        if (name.equals("ROW_NUMBER") || name.equals("RANK") || name.equals("DENSE_RANK") || name.equals("LAG") || name.equals("LEAD")) {
            running = true;
        }
        int rank = 0;
        int dense = 0;
        Object[] previousOrder = null;
        for (int position = 0; position < group.size(); position++) {
            int row = group.get(position);
            Object[] order = orderKeys.get(row);
            if (previousOrder == null || compareOrder(previousOrder, order, window.orderBy()) != 0) {
                rank = position + 1;
                dense++;
                previousOrder = order;
            }
            int from = running ? 0 : 0;
            int to = running && (window.frame() || ordered(window) || name.equals("SUM") || name.equals("COUNT")
                    || name.equals("AVG") || name.equals("MIN") || name.equals("MAX"))
                    ? position
                    : group.size() - 1;
            if (name.equals("ROW_NUMBER") || name.equals("RANK") || name.equals("DENSE_RANK") || name.equals("LAG") || name.equals("LEAD")) {
                to = position;
            }
            if (!window.frame() && !ordered(window)
                    && (name.equals("SUM") || name.equals("COUNT") || name.equals("AVG") || name.equals("MIN") || name.equals("MAX"))) {
                from = 0;
                to = group.size() - 1;
            }
            out[row] = switch (name) {
                case "ROW_NUMBER" -> (long) position + 1;
                case "RANK" -> (long) rank;
                case "DENSE_RANK" -> (long) dense;
                case "LAG" -> shift(window, group, args, position, -1);
                case "LEAD" -> shift(window, group, args, position, 1);
                case "COUNT" -> count(window, group, args, from, to);
                case "SUM" -> sum(group, args, from, to);
                case "AVG" -> avg(group, args, from, to);
                case "MIN" -> extreme(group, args, from, to, true);
                case "MAX" -> extreme(group, args, from, to, false);
                default -> throw new SeaException("unknown window function: " + window.call().name());
            };
        }
    }

    private static Object shift(WindowExpr window, List<Integer> group, List<Object> args, int position, int direction) {
        int offset = 1;
        Object fallback = null;
        List<Expr> extra = window.call().args();
        if (extra.size() >= 2 && window.constants() != null && window.constants().size() >= 2 && window.constants().get(1) instanceof Number number) {
            offset = number.intValue();
        }
        if (window.constants() != null && window.constants().size() >= 3) {
            fallback = window.constants().get(2);
        }
        int at = position + direction * offset;
        if (at < 0 || at >= group.size()) {
            return fallback;
        }
        return args.get(group.get(at));
    }

    private static Object count(WindowExpr window, List<Integer> group, List<Object> args, int from, int to) {
        if (window.call().star()) {
            return (long) (to - from + 1);
        }
        long count = 0;
        for (int i = from; i <= to; i++) {
            if (args.get(group.get(i)) != null) {
                count++;
            }
        }
        return count;
    }

    private static Object sum(List<Integer> group, List<Object> args, int from, int to) {
        BigDecimal total = null;
        boolean floating = false;
        for (int i = from; i <= to; i++) {
            Object value = args.get(group.get(i));
            if (value == null) {
                continue;
            }
            if (value instanceof Double || value instanceof Float) {
                floating = true;
            }
            BigDecimal number = Values.asDecimal(value);
            total = total == null ? number : total.add(number);
        }
        if (total == null) {
            return null;
        }
        if (floating) {
            return total.doubleValue();
        }
        try {
            return total.longValueExact();
        } catch (ArithmeticException exception) {
            return total;
        }
    }

    private static Object avg(List<Integer> group, List<Object> args, int from, int to) {
        Object total = sum(group, args, from, to);
        if (total == null) {
            return null;
        }
        long count = 0;
        for (int i = from; i <= to; i++) {
            if (args.get(group.get(i)) != null) {
                count++;
            }
        }
        if (count == 0) {
            return null;
        }
        return Values.asDecimal(total).divide(BigDecimal.valueOf(count), 8, java.math.RoundingMode.HALF_UP).stripTrailingZeros();
    }

    private static Object extreme(List<Integer> group, List<Object> args, int from, int to, boolean min) {
        Object best = null;
        for (int i = from; i <= to; i++) {
            Object value = args.get(group.get(i));
            if (value == null) {
                continue;
            }
            if (best == null) {
                best = value;
                continue;
            }
            int cmp = Values.compare(value, best);
            if (min ? cmp < 0 : cmp > 0) {
                best = value;
            }
        }
        return best;
    }

    private static boolean same(Object[] left, Object[] right) {
        if (left.length != right.length) {
            return false;
        }
        for (int i = 0; i < left.length; i++) {
            if (left[i] == null && right[i] == null) {
                continue;
            }
            if (left[i] == null || right[i] == null || Values.compare(left[i], right[i]) != 0) {
                return false;
            }
        }
        return true;
    }

    private static int compareOrder(Object[] left, Object[] right, List<OrderItem> order) {
        if (order == null || order.isEmpty()) {
            return 0;
        }
        for (int i = 0; i < order.size() && i < left.length; i++) {
            Object lv = left[i];
            Object rv = right[i];
            int cmp;
            if (lv == null && rv == null) {
                cmp = 0;
            } else if (lv == null) {
                cmp = 1;
            } else if (rv == null) {
                cmp = -1;
            } else {
                cmp = Values.compare(lv, rv);
            }
            if (order.get(i).descending()) {
                cmp = -cmp;
            }
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }
}
