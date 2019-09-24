package com.chriniko.database.core.engineer.test;

import org.junit.Test;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

public class IdGeneratorTest {

    @Test
    public void produce_works_as_expected_one_client() {

        // given
        IdGenerator idGenerator = new IdGenerator();

        // when
        Long uniqueId = idGenerator.produce();

        // then
        assertEquals(readStoredId() - IdGenerator.ID_BATCH_SIZE, uniqueId.longValue());

    }

    @Test
    public void produce_works_as_expected_multiple_clients() throws Exception {

        // given
        IdGenerator idGenerator = new IdGenerator();

        Set<Long> generatedIds = Collections.synchronizedSet(new LinkedHashSet<>());

        int clients = 3000;

        CountDownLatch finishLatch = new CountDownLatch(clients);

        ExecutorService workersPool = Executors.newFixedThreadPool(clients);

        Runnable task = () -> {
            Long producedId = idGenerator.produce();
            generatedIds.add(producedId);
            finishLatch.countDown();
        };

        // when
        for (int i=1; i<=clients; i++) {
            workersPool.submit(task);
        }

        finishLatch.await();


        // then
        assertEquals(clients, generatedIds.size());
        System.out.println("generatedIds: " + generatedIds);


        // cleanup
        workersPool.shutdown();
    }

    // --- utils ---
    private long readStoredId() {
        try (BufferedReader br = Files.newBufferedReader(Paths.get(IdGenerator.STORAGE_PATH))) {
            return Long.parseLong(br.readLine());
        } catch (NumberFormatException e) {
            throw new IdGenerator.ProcessingException("malformed last idx read from file");
        } catch (Exception e) {
            throw new IdGenerator.ProcessingException("could not read from file", e);
        }
    }

}