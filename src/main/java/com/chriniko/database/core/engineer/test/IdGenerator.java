package com.chriniko.database.core.engineer.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

public class IdGenerator {

    // IMPORTANT NOTE: Change path to a correct one based on your workstation.
    public static final String STORAGE_PATH = "/home/giatros/neo4j_id_generator.txt";

    public static final int ID_BATCH_SIZE = 100;

    private final Queue<Long> precalculatedIds = new LinkedList<>();

    private long lastId;

    public IdGenerator() {
        Path path = getPath();
        if (!path.toFile().exists()) {
            lastId = 0;
            createFile(path);
            calculateNextIds(1L, ID_BATCH_SIZE);
        } else {
            readLastId(path);
            calculateNextIds(lastId, lastId + ID_BATCH_SIZE);
        }

        lastId += ID_BATCH_SIZE;
        saveState(lastId);
    }

    public Long produce() {
        synchronized (IdGenerator.class) {
            return Optional
                    .ofNullable(precalculatedIds.poll())
                    .orElseGet(() -> {
                        long startTime = System.nanoTime();
                        try {
                            calculateNextIds(lastId, lastId + ID_BATCH_SIZE);
                            lastId += ID_BATCH_SIZE;
                            saveState(lastId);

                            //System.out.println("precalculatedIds: " + precalculatedIds);

                            return precalculatedIds.poll();
                        } finally {
                            long totalTimeInNs = System.nanoTime() - startTime;
                            long totalTimeInMs = TimeUnit.MILLISECONDS.convert(totalTimeInNs, TimeUnit.NANOSECONDS);
                            System.out.println("totalTime (ns) for produce: " + totalTimeInNs + " --- in ms: " + totalTimeInMs);
                        }
                    });
        }
    }

    private void calculateNextIds(long from, long to) {
        LongStream
                .range(from, to)
                .forEach(precalculatedIds::add);

    }

    private void saveState(long lastId) {
        Path path = getPath();
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(path, StandardOpenOption.TRUNCATE_EXISTING)) {
            bufferedWriter.write(String.valueOf(lastId));
            bufferedWriter.flush();
        } catch (Exception e) {
            throw new ProcessingException("could not write to file", e);
        }
    }

    private Path getPath() {
        return Paths.get(STORAGE_PATH);
    }

    private void readLastId(Path path) {
        try (BufferedReader br = Files.newBufferedReader(path)) {
            lastId = Long.parseLong(br.readLine());
        } catch (NumberFormatException e) {
            throw new ProcessingException("malformed last idx read from file");
        } catch (Exception e) {
            throw new ProcessingException("could not read from file", e);
        }
    }

    private void createFile(Path path) {
        try {
            Files.createFile(path);
        } catch (IOException e) {
            throw new ProcessingException("could not create storage file", e);
        }
    }

    // --- internals ---

    public static class ProcessingException extends RuntimeException {
        public ProcessingException(String message, Throwable error) {
            super(message, error);
        }

        public ProcessingException(String message) {
            super(message);
        }
    }


}
