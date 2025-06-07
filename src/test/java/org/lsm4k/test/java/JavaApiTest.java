package org.lsm4k.test.java;

import kotlin.io.FilesKt;
import org.junit.jupiter.api.Test;
import org.lsm4k.api.DatabaseEngine;
import org.lsm4k.api.LSM4KConfiguration;
import org.lsm4k.api.compaction.CompactionStrategy;
import org.lsm4k.api.compaction.TieredCompactionStrategy;
import org.lsm4k.util.bytes.Bytes;
import org.lsm4k.util.cursor.Cursor;
import org.lsm4k.util.unit.BinarySize;
import org.lsm4k.util.unit.SizeUnit;

import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.*;

public class JavaApiTest {

    @Test
    public void canUseDatabaseEngine() throws Exception {
        var tempDir = Files.createTempDirectory("lsm4k-test").toFile();
        try {
            try (var engine = DatabaseEngine.openOnDirectory(tempDir)) {
                engine.readWriteTransaction((tx) -> {
                    var store = tx.createNewStore("test");
                    store.put(Bytes.of("hello"), Bytes.of("world"));
                    store.put(Bytes.of("foo"), Bytes.of("bar"));
                    return tx.commit();
                });

                engine.readOnlyTransaction((tx) -> {
                    var store = tx.getStore("test");
                    var entries = store.useCursor(Cursor::listAllEntriesAscending);
                    assertEquals(2, entries.size());

                    var firstEntry = entries.get(0);
                    assertEquals(firstEntry.getFirst(), Bytes.of("foo"));
                    assertEquals(firstEntry.getSecond(), Bytes.of("bar"));

                    var secondEntry = entries.get(1);
                    assertEquals(secondEntry.getFirst(), Bytes.of("hello"));
                    assertEquals(secondEntry.getSecond(), Bytes.of("world"));

                    return 0;
                });
            }
        } finally {
            FilesKt.deleteRecursively(tempDir);
        }
    }

    @Test
    public void canCreateConfiguration() {
        var config = LSM4KConfiguration.builder()
            .withMaxBlockSize(16, SizeUnit.MEBIBYTE)
            .withDefaultCompactionStrategy(
                CompactionStrategy.tieredBuilder()
                    .withSizeRatio(2.5)
                    .build()
            )
            .build();

        assertEquals(new BinarySize(16, SizeUnit.MEBIBYTE), config.getMaxBlockSize());
        assertInstanceOf(TieredCompactionStrategy.class, config.getDefaultCompactionStrategy());

    }

}
