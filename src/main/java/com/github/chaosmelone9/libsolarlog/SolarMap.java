package com.github.chaosmelone9.libsolarlog;

import com.github.chaosmelone9.libsolarlog.dataExtraction.ExtractFromJS;
import com.github.chaosmelone9.libsolarlog.dataExtraction.GetConfigurationFromBaseVars;
import com.github.chaosmelone9.libsolarlog.databaseInteraction.MariaDBInteraction;
import com.github.chaosmelone9.libsolarlog.ftpInteraction.FTPServerInteraction;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;

/**
 * Main Class for runtime data storage
 * @author Christoph Kohnen
 * @since 0.0.0rc0.2-0
 */
public class SolarMap {
    private final Map<Inverter, Map<Date, Map<String, Integer>>> data;

    public SolarMap() {
        this.data = new HashMap<>();
    }

    public void addFromMap(Map<Inverter, Map<Date, Map<String, Integer>>> map) {
        map.forEach((inverter, dateMapMap) -> {
            if (!data.containsKey(inverter)) {
                data.put(inverter, dateMapMap);
            } else {
                addData(inverter, dateMapMap);
            }
        });
    }

    public void addFromJSFile(List<Inverter> inverters, File file) throws UnsupportedInverterFunctionException, IOException, ParseException {
        addFromMap(ExtractFromJS.extractDataFromJSFile(inverters, file));
    }

    public List<File> addFromJSFiles(List<Inverter> inverters, List<File> files) {
        List<File> brokenFiles = new ArrayList<>();
        for (File file : files) {
            try {
                addFromJSFile(inverters, file);
            } catch (UnsupportedInverterFunctionException | IOException | ParseException | NullPointerException | NumberFormatException e) {
                brokenFiles.add(file);
            }
        }
        return brokenFiles;
    }

    public List<File> addFromJSFiles(File base_vars, List<File> files) throws IOException {
        return addFromJSFiles(GetConfigurationFromBaseVars.getInvertersFromBaseVars(base_vars), files);
    }

    public List<File> addFromFTPServer(String host, String user, String password) throws IOException {
        return addFromJSFiles(FTPServerInteraction.getBaseVarsFromFTPServer(host, user, password), FTPServerInteraction.getJSFilesFromFTPServer(host, user, password));
    }

    public void addFromMariaDB(String user, String password, String address, String database) throws SQLException {
        addFromMap(MariaDBInteraction.getFromMariaDB(user, password, address, database));
    }

    public void pushToMariaDB(String user, String password, String address, String database, boolean ignoreFlag) throws SQLException {
        MariaDBInteraction.pushToMariaDB(user, password, address, database, getData(), ignoreFlag);
    }

    private void addData(Inverter inverter, Map<Date, Map<String, Integer>> map) {
        map.forEach(data.get(inverter)::putIfAbsent);
    }

    public Map<Inverter, Map<Date, Map<String, Integer>>> getData() {
        return data;
    }

    @Override
    public String toString() {
        return data.toString();
    }
}
