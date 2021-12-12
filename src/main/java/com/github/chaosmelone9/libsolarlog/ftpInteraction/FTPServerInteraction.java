package com.github.chaosmelone9.libsolarlog.ftpInteraction;


import com.github.chaosmelone9.libsolarlog.fileInteraction.WorkingDirectory;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Functions for retrieving files from FTP-Server
 * @author Christoph Kohnen
 * @since 0.0.0rc3-0
 */
public class FTPServerInteraction {
    public static File getBaseVarsFromFTPServer(String host, String user, String password) throws IOException {
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(host);
        int reply = ftpClient.getReplyCode();
        if (!FTPReply.isPositiveCompletion(reply)) {
            ftpClient.disconnect();
            throw new IOException("Exception in connecting to FTP Server");
        }
        ftpClient.login(user, password);
        FTPFile[] files = ftpClient.listFiles();
        FTPFile base_vars_ftp = null;
        for (FTPFile file : files) {
            if(file.getName().startsWith("base_vars")) {
                base_vars_ftp = file;
            }
        }

        File downloadDirectory = new File(String.valueOf(Paths.get(WorkingDirectory.getDirectory().getPath(), "FTP")));
        if(!downloadDirectory.exists()) {
            if(!downloadDirectory.mkdirs()) {
                throw new IOException("Can't access /tmp/");
            }
        }
        if(base_vars_ftp == null) {
            throw new IOException("No such file on FTPServer!");
        }
        File base_vars = new File(downloadDirectory, base_vars_ftp.getName());
        if(!base_vars.exists()) {
            FileOutputStream out = new FileOutputStream(base_vars);
            ftpClient.retrieveFile(base_vars_ftp.getName(), out);
        }
        ftpClient.disconnect();
        return base_vars;
    }

    public static List<File> getJSFilesFromFTPServer(String host, String user, String password) throws IOException {
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(host);
        int reply = ftpClient.getReplyCode();
        if (!FTPReply.isPositiveCompletion(reply)) {
            ftpClient.disconnect();
            throw new IOException("Exception in connecting to FTP Server");
        }
        ftpClient.login(user, password);
        FTPFile[] files = ftpClient.listFiles();
        List<FTPFile> minuteFiles = new ArrayList<>();
        for (FTPFile file : files) {
            if(file.getName().startsWith("min") && !(file.getName().contains("day") || file.getName().contains("cur"))) {
                minuteFiles.add(file);
            }
        }

        List<File> jsFiles = new ArrayList<>();
        File downloadDirectory = new File(String.valueOf(Paths.get(WorkingDirectory.getDirectory().getPath(), "FTP")));
        if(!downloadDirectory.exists()) {
            if(!downloadDirectory.mkdirs()) {
                throw new IOException("Can't access /tmp/");
            }
        }
        for (FTPFile minuteFile : minuteFiles) {
            File download = new File(downloadDirectory, minuteFile.getName());
            if(!download.exists()) {
                FileOutputStream out = new FileOutputStream(download);
                ftpClient.retrieveFile(minuteFile.getName(), out);
            }
            jsFiles.add(download);
        }
        ftpClient.disconnect();
        return jsFiles;
    }
}
