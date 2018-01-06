package ru.mail.polis;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public final class ServerEnlighten2 {
    private static final int PORT = 8080;

    private ServerEnlighten2() {
        // Not instantiable
    }

    public static void main(String[] args) throws IOException {
        // Temporary storage in the file system
        final File data = Files.createTempDirectory();

        final Set<String> topology = new HashSet<>(3);
        topology.add("http://10.213.154.1:8080");
        topology.add("http://10.213.154.185:8080");
        topology.add("http://10.213.154.206:8080");

        // Start the storage
        final KVService storage =
                KVServiceFactory.createEnlighten(
                        PORT,
                        data,
                        topology,
                        "http://10.213.154.185:8080");
        storage.start();
        Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
    }
}

