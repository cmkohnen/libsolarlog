package de.cmkohnen.libsolarlog.fileInteraction;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.NotDirectoryException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Functions for filtering a list of files
 * @author Christoph Kohnen
 * @since 0.0.0rc2-0
 */
public class FilterFiles {
    public static List<File> getJSFilesInDirectory(File directory) throws NotDirectoryException, FileNotFoundException {
        if (!directory.isDirectory()) {
            throw new NotDirectoryException(directory.getName() + " is not a directory!");
        } else if (!directory.exists()) {
            throw new FileNotFoundException(directory.getName() + " does not exist!");
        }
        return Arrays.asList(Objects.requireNonNull(directory.listFiles((dir1, name) -> name.toLowerCase().endsWith(".js") && (name.startsWith("min") && !(name.contains("day") || name.contains("cur"))))));
    }
}
