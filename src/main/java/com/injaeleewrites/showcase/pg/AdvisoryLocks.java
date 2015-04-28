package com.injaeleewrites.showcase.pg;

import com.injaeleewrites.showcase.pg.utility.DBConnectionBuilder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author eksor
 * @since 20150427
 */
public class AdvisoryLocks {

    public static class Worker implements Runnable {
        private static int counter = 0;
        private final int myId;
        private final DataSource dataSource;

        private int observedCount = -1;

        public Worker(int id, DataSource ds) {
            myId = id;
            dataSource = ds;
        }

        @Override
        public void run() {
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false); // key to starting a transaction
                Statement stmt = conn.createStatement();
                System.out.printf("[%d][%d] About to get lock.%n", System.nanoTime(), myId);
                stmt.execute("SELECT pg_advisory_xact_lock(1)");
                System.out.printf("[%d][%d] Got lock with counter [%d]. " +
                        "Going to sleep for 1 second.%n", System.nanoTime(), myId, counter);
                ++counter;
                Thread.sleep(1);
                observedCount = counter;
                System.out.printf("[%d][%d] Done sleeping for 1 second and the counter is [%d].%n",
                        System.nanoTime(), myId, counter);
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        public int getObservedCount() {
            return observedCount;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        DataSource ds = DBConnectionBuilder.BoneCPBuilder
                .initialize()
                .withDriver("org.postgresql.Driver")
                .withJDBCURL("jdbc:postgresql://localhost:5433/eksor")
                .withUsername("eksor")
                .withPassword("")
                .build();

        Collection<Worker> workers = new ArrayList<>();
        int workerCount = 300;
        int coreCount = Runtime.getRuntime().availableProcessors();
        System.out.printf("There are [%d] cores.%n", coreCount);
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        for (int i = 0; i < workerCount; ++i) {
            Worker w = new AdvisoryLocks.Worker(i, ds);
            workers.add(w);
            executorService.execute(w);
        }
        executorService.awaitTermination(4, TimeUnit.SECONDS);
        executorService.shutdown();

        Set<Integer> countCheck = new HashSet<>(workers.size());
        for (Worker w : workers) {
            if (countCheck.contains(w.getObservedCount())) {
                System.err.printf("Lock wasn't working as designed.");
                System.exit(1);
            } else {
                countCheck.add(w.getObservedCount());
            }
        }
        System.out.printf("Lock worked as designed.");
    }
}
