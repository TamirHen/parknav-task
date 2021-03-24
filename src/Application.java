import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Application {

    public static List<Stream<Timestamp>> bucketizeTimestamps(Stream<Timestamp> timestampStream) {
        List<Stream<Timestamp>> buckets = new ArrayList<>();
        List<Long> times = timestampStream.sorted().map(Timestamp::getTime).collect(Collectors.toList());
        if (times.size() == 0) return buckets;
        long firstInBucket = times.get(0);
        buckets.add(Stream.of(new Timestamp(firstInBucket)));
        boolean isFirst = true;
        for (long timeInMilliseconds : times) {
            // Checks if the time difference between the current timestamp and the first-in-bucket timestamp is less then 30 minutes:
            if ((timeInMilliseconds/60 - firstInBucket/60)/1000 < 30) {
                // Prevents duplicate values when the first timestamp has its own bucket:
                if (isFirst) {
                    isFirst = false;
                    continue;
                }
                // Add timestamp to the last stream:
                Stream<Timestamp> lastStream = buckets.get(buckets.size() - 1);
                buckets.set(buckets.size() - 1, Stream.concat(lastStream, Stream.of(new Timestamp(timeInMilliseconds))));
            } else {
                // Create new bucket:
                buckets.add(Stream.of(new Timestamp(timeInMilliseconds)));
                firstInBucket = timeInMilliseconds;
                isFirst = false;
            }
        }
        return buckets;
    }

    public static void main(String[] args) {
        Timestamp p1 = Timestamp.valueOf("2000-01-01 13:30:00.0");
        Timestamp p2 = Timestamp.valueOf("2000-01-01 14:50:00.0");
        Timestamp p3 = Timestamp.valueOf("2000-01-01 17:00:00.0");
        Timestamp p4 = Timestamp.valueOf("2000-01-01 14:41:00.0");
        Timestamp p5 = Timestamp.valueOf("2000-01-01 16:23:00.0");
        Timestamp p6 = Timestamp.valueOf("2000-01-01 14:00:00.0");
        Timestamp p7 = Timestamp.valueOf("2000-01-01 14:10:00.0");
        Timestamp p8 = Timestamp.valueOf("2000-01-01 14:50:00.0");
        Timestamp p9 = Timestamp.valueOf("2000-01-01 13:30:00.0");
        List<Stream<Timestamp>> buckets = bucketizeTimestamps(Stream.of(p1, p2, p3, p4, p5, p6, p7, p8, p9));
        System.out.print("\n\n");
        buckets.forEach(bucket -> {
            bucket.forEach(time -> System.out.print(time + "   "));
            System.out.print("\n\n");
        });
    }

}

