package ru.mail.polis.Kubrin;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.NoSuchElementException;

/**
 * Created by Egor on 09.10.2017.
 */
public class DaoFiles implements Dao {
    private static File directory;

    public DaoFiles(File directory) {
        this.directory = directory;
    }

    @NotNull
    @Override
    public byte[] getData(@NotNull String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        File file = new File(directory, key);
        if(!file.exists())
            throw new NoSuchElementException();
        final byte[] data = new byte[(int) file.length()];
        try (InputStream inputStream = new FileInputStream(file)) {
            if(inputStream.read(data) != file.length())
                throw new IOException();
        }
        return data;
    }

    @NotNull
    @Override
    public void upsertData(@NotNull String key, @NotNull byte[] data) throws IllegalArgumentException, IOException {
        try (OutputStream outputStream = new FileOutputStream(new File(directory, key))) {
            outputStream.write(data);
        }
    }

    @NotNull
    @Override
    public void deleteData(@NotNull String key) throws IllegalArgumentException, IOException {
        new File(directory, key).delete();
    }
}
