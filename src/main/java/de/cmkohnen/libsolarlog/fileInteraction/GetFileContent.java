package de.cmkohnen.libsolarlog.fileInteraction;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Get content of any File
 * @author Christoph Kohnen
 * @since 0.0.0rc0.3-0
 */
public class GetFileContent {
    public static String getFileContentAsString(File file) throws IOException {
        return new String(Files.readAllBytes(Paths.get(file.getPath())));
    }

    public static List<String> getFileContentAsList(File file) throws IOException {
        return Arrays.asList(getFileContentAsString(file).split("\n"));
    }
}
