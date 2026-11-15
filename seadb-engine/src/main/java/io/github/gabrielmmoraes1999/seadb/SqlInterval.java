package io.github.gabrielmmoraes1999.seadb;

/**
 * Day-to-second interval stored as a signed number of milliseconds.
 */
public final class SqlInterval {

    public final long millis;

    public SqlInterval(long millis) {
        this.millis = millis;
    }

    public static SqlInterval parse(String text) {
        if (text == null) {
            throw new SeaException("invalid INTERVAL");
        }
        String trimmed = text.trim();
        boolean negative = trimmed.startsWith("-");
        if (negative || trimmed.startsWith("+")) {
            trimmed = trimmed.substring(1).trim();
        }
        String[] dayAndTime = trimmed.split(" ", 2);
        long days;
        String clock;
        if (dayAndTime.length == 2) {
            try {
                days = Long.parseLong(dayAndTime[0]);
            } catch (NumberFormatException exception) {
                throw new SeaException("invalid INTERVAL");
            }
            clock = dayAndTime[1];
        } else {
            days = 0;
            clock = trimmed;
        }
        String[] parts = clock.split(":");
        if (parts.length != 3) {
            throw new SeaException("invalid INTERVAL");
        }
        long hours;
        long minutes;
        long seconds;
        long fraction = 0;
        try {
            hours = Long.parseLong(parts[0]);
            minutes = Long.parseLong(parts[1]);
            String second = parts[2];
            int dot = second.indexOf('.');
            if (dot >= 0) {
                seconds = Long.parseLong(second.substring(0, dot));
                String frac = second.substring(dot + 1);
                if (frac.length() > 3) {
                    frac = frac.substring(0, 3);
                }
                while (frac.length() < 3) {
                    frac = frac + "0";
                }
                fraction = Long.parseLong(frac);
            } else {
                seconds = Long.parseLong(second);
            }
        } catch (NumberFormatException exception) {
            throw new SeaException("invalid INTERVAL");
        }
        if (hours > 23 || minutes > 59 || seconds > 59) {
            throw new SeaException("invalid INTERVAL");
        }
        long millis = days * 86_400_000L + hours * 3_600_000L + minutes * 60_000L + seconds * 1_000L + fraction;
        if (negative) {
            millis = -millis;
        }
        return new SqlInterval(millis);
    }

    @Override
    public String toString() {
        long value = millis;
        String sign = "";
        if (value < 0) {
            sign = "-";
            value = -value;
        }
        long days = value / 86_400_000L;
        long rest = value % 86_400_000L;
        long hours = rest / 3_600_000L;
        rest %= 3_600_000L;
        long minutes = rest / 60_000L;
        rest %= 60_000L;
        long seconds = rest / 1_000L;
        long fraction = rest % 1_000L;
        String clock = String.format("%02d:%02d:%02d", hours, minutes, seconds);
        if (fraction != 0) {
            clock = clock + String.format(".%03d", fraction);
        }
        return sign + days + " " + clock;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof SqlInterval interval && interval.millis == millis;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(millis);
    }
}
