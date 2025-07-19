package de.cmkohnen.libsolarlog.fileInteraction;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * FUnctions for temporary file storage
 * @author Christoph Kohnen
 * @since 0.0.0rc3-0
 */
public class WorkingDirectory {
    private static File workingDirectory = null;

    public static File getDirectory() throws IOException {
        if(workingDirectory == null) {
            workingDirectory = new File(Paths.get("/tmp/").toAbsolutePath().toString(), "libsolarlog");
            if(!workingDirectory.exists()) {
                if(!workingDirectory.mkdirs()) {
                    throw new IOException("Can't access /tmp/");
                }
            }
        }
        return workingDirectory;
    }

    public static void clear() throws IOException {
        File file = getDirectory();
        for (File subFile : Objects.requireNonNull(file.listFiles())) {
            if(subFile.isDirectory()) {
                deleteFolder(subFile);
            } else {
                if(!subFile.delete()) {
                    throw new IOException("Can't delete " + subFile);
                }
            }
        }
    }

    public static void setDirectory(File directory) throws FileNotFoundException, NotDirectoryException {
        if(directory.isDirectory()) {
            if(!directory.exists()) {
                throw new FileNotFoundException();
            } else workingDirectory = directory;
        } else throw new NotDirectoryException(String.format("%s is no directory.",directory.getName()));
    }

    private static void deleteFolder(File file) throws IOException {
        for (File subFile : Objects.requireNonNull(file.listFiles())) {
            if(subFile.isDirectory()) {
                deleteFolder(subFile);
            } else {
                if(!subFile.delete()) {
                    throw new IOException("Can't delete " + subFile.getName());
                }
            }
        }
        if(!file.delete()) {
            throw new IOException("Can't delete " + file.getName());
        }
    }
}
