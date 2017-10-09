package ru.mail.polis.Kubrin;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Created by Egor on 09.10.2017.
 */
public interface Dao {
    @NotNull
    byte[] getData(@NotNull String key) throws NoSuchElementException, IllegalArgumentException, IOException;

    @NotNull
    void upsertData(@NotNull String key, @NotNull byte[] data) throws IllegalArgumentException, IOException;

    @NotNull
    void deleteData(@NotNull String key) throws IllegalArgumentException, IOException;
}
