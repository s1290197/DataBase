package com.example;

import java.io.*;
import java.lang.management.*;
import java.util.*;

import javax.cache.Cache;

import org.apache.ignite.*;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;

/**
 * IgniteDist is a Java application for processing and verifying distributed data
 * in an Apache Ignite cluster. The program demonstrates how to:
 * - Configure a cluster with multiple nodes.
 * - Insert and process 5-minute and 1-hour interval datasets.
 * - Measure performance and memory usage.
 * - Verify data distribution across cluster nodes.
 */
public class IgniteDist {

    public static void main(String[] args) {
        // Ignite Configuration
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(true);
        cfg.setPeerClassLoadingEnabled(true);

        // Add TcpDiscoverySpi with static IP finder
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList(
            "163.143.165.154:47500..47509", // Main server
            "163.143.165.155:47500..47509", // Slave 1
            "163.143.165.160:47500..47509"  // Slave 2
        ));
        discoverySpi.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(discoverySpi);

        // Start Ignite node
        try (Ignite ignite = Ignition.start(cfg)) {
            System.out.println("Ignite node started.");

            // Activate the cluster
            ignite.cluster().state(ClusterState.ACTIVE);

            // Verify node connections
            checkNodeConnections(ignite);

            // Process 5-minute interval data
            process5MinData(ignite);

            // Process 1-hour interval data
            process1HourData(ignite);

            // Verify data distribution across nodes
            verifyDataOnEachNode(ignite, "5min_congestion");
            verifyDataOnEachNode(ignite, "1hour_congestion");
        }
    }

    /**
     * Verifies the connection to all nodes in the Ignite cluster.
     *
     * @param ignite Ignite instance for the cluster.
     */
    private static void checkNodeConnections(Ignite ignite) {
        System.out.println("Checking connections to all cluster nodes...");

        ignite.cluster().forServers().nodes().forEach(node -> {
            System.out.println("Connected to node: " + node.id());
            System.out.println("Node address: " + node.addresses());
        });
    }

    /**
     * Verifies data distribution for a specific cache across all nodes.
     *
     * @param ignite Ignite instance for the cluster.
     * @param cacheName Name of the cache to verify.
     */
    private static void verifyDataOnEachNode(Ignite ignite, String cacheName) {
        System.out.println("Verifying data distribution for cache: " + cacheName);

        IgniteCache<Long, Map<String, Object>> cache = ignite.cache(cacheName);
        if (cache == null) {
            System.out.println("Cache not found: " + cacheName);
            return;
        }

        ignite.cluster().forServers().nodes().forEach(node -> {
            System.out.println("Node ID: " + node.id());

            // Use ScanQuery to retrieve local data
            try (QueryCursor<Cache.Entry<Long, Map<String, Object>>> cursor = cache.query(new ScanQuery<>())) {
                System.out.println("Sample data on node " + node.id() + ":");
                int count = 0;
                for (Cache.Entry<Long, Map<String, Object>> entry : cursor) {
                    System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
                    if (++count >= 5) break; // Display only 5 entries
                }
            }
        });
    }

    /**
     * Processes and inserts 5-minute interval data into the Ignite cluster.
     *
     * @param ignite Ignite instance for the cluster.
     */
    private static void process5MinData(Ignite ignite) {
        CacheConfiguration<Long, Map<String, Object>> congestionCfg = new CacheConfiguration<>("5min_congestion");
        IgniteCache<Long, Map<String, Object>> congestionCache = ignite.getOrCreateCache(congestionCfg);

        long startTime = System.nanoTime();

        // Insert data from TSV
        insert5MinDataFromTsv(congestionCache);

        long endTime = System.nanoTime();
        double elapsedTime = (endTime - startTime) / 1e6; // Convert to milliseconds

        // Measure memory usage
        long rss = getRSSMemory();
        long uss = getHeapMemory();

        // Write results to CSV
        writeResultsToCsv("5min_result.csv", elapsedTime, uss, rss);

        // Verify cache contents
        System.out.println("5min_congestion cache size: " + congestionCache.size());
        try (QueryCursor<Cache.Entry<Long, Map<String, Object>>> cursor =
                 congestionCache.query(new ScanQuery<>())) {
            for (Cache.Entry<Long, Map<String, Object>> entry : cursor) {
                System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
            }
        }
    }

    /**
     * Processes and inserts 1-hour interval data into the Ignite cluster.
     *
     * @param ignite Ignite instance for the cluster.
     */
    private static void process1HourData(Ignite ignite) {
        CacheConfiguration<Long, Map<String, Object>> congestionCfg = new CacheConfiguration<>("1hour_congestion");
        IgniteCache<Long, Map<String, Object>> congestionCache = ignite.getOrCreateCache(congestionCfg);

        long startTime = System.nanoTime();

        // Insert data from CSV
        insert1HourDataFromCsv(congestionCache);

        long endTime = System.nanoTime();
        double elapsedTime = (endTime - startTime) / 1e6; // Convert to milliseconds

        // Measure memory usage
        long rss = getRSSMemory();
        long uss = getHeapMemory();

        // Write results to CSV
        writeResultsToCsv("1hour_result.csv", elapsedTime, uss, rss);

        // Verify cache contents
        System.out.println("1hour_congestion cache size: " + congestionCache.size());
        try (QueryCursor<Cache.Entry<Long, Map<String, Object>>> cursor =
                 congestionCache.query(new ScanQuery<>())) {
            for (Cache.Entry<Long, Map<String, Object>> entry : cursor) {
                System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
            }
        }
    }

    /**
     * Inserts 5-minute interval data from a TSV file into the specified cache.
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

            // Skip header
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
     * Inserts 1-hour interval data from a CSV file into the specified cache.
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

            // Skip header
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
     * @param fileName Name of the output CSV file.
     * @param time Time taken for the operation in milliseconds.
     * @param uss Unique Set Size (USS) memory usage in KB.
     * @param rss Resident Set Size (RSS) memory usage in KB.
     */
    private static void writeResultsToCsv(String fileName, double time, long uss, long rss) {
        try (FileWriter writer = new FileWriter(fileName)) {
            writer.write("Operation,Time (ms),USS (KB),RSS (KB),PeakHeap (KB)\n");
            writer.write(String.format("%s,%.3f,%d,%d\n", fileName.replace("_result.csv", ""), time, uss, rss));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Retrieves heap memory usage (USS) in KB.
     *
     * @return Heap memory usage in KB.
     */
    private static long getHeapMemory() {
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        return heapUsage.getUsed() / 1024;
    }

    /**
     * Retrieves Resident Set Size (RSS) memory usage in KB.
     *
     * @return RSS memory usage in KB, or -1 if retrieval fails.
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
