import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 * IgniteInsert is a Java program that utilizes Apache Ignite to process and store
 * data from 5-minute and 1-hour interval datasets. The program demonstrates how to:
 * - Configure Ignite with a static IP-based discovery mechanism.
 * - Insert data into Ignite caches.
 * - Measure execution time and memory usage (USS and RSS).
 * - Write results to CSV files for analysis.
 * - Verify cache contents using ScanQuery.
 */
public class InsertIgnite {

    public static void main(String[] args) {
        // Configure Apache Ignite with client mode and discovery settings
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(true);
        cfg.setPeerClassLoadingEnabled(true);

        // Configure static IP discovery
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509")); // Static IPs
        discoverySpi.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(discoverySpi);

        // Start Ignite node
        try (Ignite ignite = Ignition.start(cfg)) {
            // Process 5-minute interval data
            process5MinData(ignite);

            // Process 1-hour interval data
            process1HourData(ignite);
        }
    }

    /**
     * Processes 5-minute interval data by inserting it into an Ignite cache
     * and measuring performance and memory usage.
     *
     * @param ignite Ignite instance to use for cache operations.
     */
    private static void process5MinData(Ignite ignite) {
        CacheConfiguration<Long, Map<String, Object>> congestionCfg = new CacheConfiguration<>("5min_congestion");
        IgniteCache<Long, Map<String, Object>> congestionCache = ignite.getOrCreateCache(congestionCfg);

        long startTime = System.nanoTime();

        // Insert data from TSV file into the cache
        insert5MinDataFromTsv(congestionCache);

        long endTime = System.nanoTime();
        double elapsedTime = (endTime - startTime) / 1e6; // Convert to milliseconds

        // Measure memory usage
        long rss = getRSSMemory();
        long uss = getHeapMemory();

        // Write results to CSV
        writeResultsToCsv("5min_result.csv", elapsedTime, uss, rss);

        // Verify cache contents using ScanQuery
        System.out.println("5min_congestion cache size: " + congestionCache.size());
        try (QueryCursor<Cache.Entry<Long, Map<String, Object>>> cursor =
                 congestionCache.query(new ScanQuery<>())) {
            for (Cache.Entry<Long, Map<String, Object>> entry : cursor) {
                System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
            }
        }
    }

    /**
     * Processes 1-hour interval data by inserting it into an Ignite cache
     * and measuring performance and memory usage.
     *
     * @param ignite Ignite instance to use for cache operations.
     */
    private static void process1HourData(Ignite ignite) {
        CacheConfiguration<Long, Map<String, Object>> congestionCfg = new CacheConfiguration<>("1hour_congestion");
        IgniteCache<Long, Map<String, Object>> congestionCache = ignite.getOrCreateCache(congestionCfg);

        long startTime = System.nanoTime();

        // Insert data from CSV file into the cache
        insert1HourDataFromCsv(congestionCache);

        long endTime = System.nanoTime();
        double elapsedTime = (endTime - startTime) / 1e6; // Convert to milliseconds

        // Measure memory usage
        long rss = getRSSMemory();
        long uss = getHeapMemory();

        // Write results to CSV
        writeResultsToCsv("1hour_results.csv", elapsedTime, uss, rss);

        // Verify cache contents using ScanQuery
        System.out.println("1hour_congestion cache size: " + congestionCache.size());
        try (QueryCursor<Cache.Entry<Long, Map<String, Object>>> cursor =
                 congestionCache.query(new ScanQuery<>())) {
            for (Cache.Entry<Long, Map<String, Object>> entry : cursor) {
                System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
            }
        }
    }

    /**
     * Inserts 5-minute interval data from a TSV file into the specified Ignite cache.
     *
     * @param congestionCache Ignite cache for storing congestion data.
     */
    private static void insert5MinDataFromTsv(IgniteCache<Long, Map<String, Object>> congestionCache) {
        String tsvFile = "../data/5minFukushimaActualCongestionData.tsv";
        File file = new File(tsvFile);
        System.out.println("Checking file: " + file.getAbsolutePath());
        if (!file.exists()) {
            System.err.println("File does not exist: " + file.getAbsolutePath());
            return;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;

            // Skip the header line
            if ((line = br.readLine()) != null) {
                System.out.println("Header: " + line);
            }

            long id = 1;
            while ((line = br.readLine()) != null) {
                String[] values = line.split("\t");

                try {
                    Map<String, Object> congestionData = new HashMap<>();
                    congestionData.put("offer_date", values[1]);
                    congestionData.put("offer_hour", values[3].isEmpty() ? 0 : Integer.parseInt(values[3]));

                    congestionCache.put(id, congestionData);
                    id++;
                } catch (NumberFormatException e) {
                    System.err.println("Invalid number format at line: " + line);
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Inserts 1-hour interval data from a CSV file into the specified Ignite cache.
     *
     * @param congestionCache Ignite cache for storing congestion data.
     */
    private static void insert1HourDataFromCsv(IgniteCache<Long, Map<String, Object>> congestionCache) {
        String csvFile = "../data/congestion.csv";
        File file = new File(csvFile);
        System.out.println("Checking file: " + file.getAbsolutePath());
        if (!file.exists()) {
            System.err.println("File does not exist: " + file.getAbsolutePath());
            return;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;

            // Skip the header line
            if ((line = br.readLine()) != null) {
                System.out.println("Header: " + line);
            }

            long id = 1;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");

                try {
                    Map<String, Object> congestionData = new HashMap<>();
                    congestionData.put("time", values[3]);

                    double congestionTime = values[17].isEmpty() ? 0.0 : Double.parseDouble(values[17]);
                    congestionData.put("congestionTime", congestionTime);

                    congestionCache.put(id, congestionData);
                    id++;
                } catch (NumberFormatException e) {
                    System.err.println("Invalid number format at line: " + line);
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Writes performance and memory usage results to a CSV file.
     *
     * @param fileName Name of the CSV file to write results to.
     * @param time Time taken for the operation in milliseconds.
     * @param uss Unique Set Size (USS) memory usage in KB.
     * @param rss Resident Set Size (RSS) memory usage in KB.
     */
    private static void writeResultsToCsv(String fileName, double time, long uss, long rss) {
        try (FileWriter writer = new FileWriter(fileName)) {
            writer.write("Operation,Time (ms),USS (KB),RSS (KB),PeakHeap (KB)\n");
            writer.write(String.format("%s,%.3f,%d,%d\n", fileName.replace("_results.csv", ""), time, uss, rss));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Retrieves the current heap memory usage (USS) in KB.
     *
     * @return Heap memory usage in KB.
     */
    private static long getHeapMemory() {
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        return heapUsage.getUsed() / 1024;
    }

    /**
     * Retrieves the current Resident Set Size (RSS) memory usage in KB.
     *
     * @return RSS memory usage in KB, or -1 if unable to retrieve.
     */
    private static long getRSSMemory() {
        try {
            String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
            Process process = new ProcessBuilder("ps", "-o", "rss=", "-p", pid).start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line = reader.readLine();
                if (line != null && !line.trim().isEmpty()) {
                    return Long.parseLong(line.trim());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }
}
