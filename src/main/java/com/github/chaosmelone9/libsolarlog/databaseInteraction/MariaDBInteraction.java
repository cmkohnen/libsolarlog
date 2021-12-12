package com.github.chaosmelone9.libsolarlog.databaseInteraction;

import com.github.chaosmelone9.libsolarlog.Inverter;

import java.sql.*;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * Extracting data from .js files found on ftp-backups
 * @author Christoph Kohnen
 * @since 0.0.0rc0.5-0
 */
public class MariaDBInteraction {

    public static void pushToMariaDB(String user, String password, String address, String database, Map<Inverter, Map<Date, Map<String, Integer>>> data, boolean ignoreFlag) throws SQLException {
        Properties connConfig = new Properties();
        connConfig.setProperty("user", user);
        connConfig.setProperty("password", password);
        pushToMariaDB(connConfig, address, database, data, ignoreFlag);
    }

    public static void pushToMariaDB(Properties connConfig, String address, String database, Map<Inverter, Map<Date, Map<String, Integer>>> data, boolean ignoreFlag) throws SQLException {
        Connection connection = DriverManager.getConnection(address, connConfig);
        connection.setAutoCommit(false);

        // Create inverter Table
        Statement statement = connection.createStatement();
        statement.execute("USE `" + database + "`");
        statement.execute("CREATE TABLE IF NOT EXISTS inverter(" +
                "inverter_identifier VARCHAR (50) NOT NULL," +
                "PRIMARY KEY (inverter_identifier)," +
                "inverter_type VARCHAR (50) NOT NULL," +
                "strings INT NOT NULL," +
                "function INT NOT NULL," +
                "function_type INT NOT NULL," +
                "temperature TINYINT(1) NOT NULL" +
                ")");

        data.forEach((inverter, dateMapMap) -> {
            try {
                //Create table for Inverter
                statement.execute(createTableQuery(inverter));

                //Add Inverter to "inverter" Table
                StringBuilder inverterStatementQuery = new StringBuilder("INSERT ");
                if (ignoreFlag) {
                    inverterStatementQuery.append("IGNORE ");
                }
                inverterStatementQuery.append("INTO inverter (inverter_identifier, inverter_type, strings, function, function_type, temperature) VALUES (?, ?, ?, ?, ?, ?)");
                PreparedStatement insertInverterStatement = connection.prepareStatement(inverterStatementQuery.toString(), Statement.RETURN_GENERATED_KEYS);
                insertInverterStatement.setString(1, inverter.identifier);
                insertInverterStatement.setString(2, inverter.type);
                insertInverterStatement.setInt(3, inverter.strings);
                insertInverterStatement.setInt(4, inverter.function.ordinal());
                insertInverterStatement.setInt(5, inverter.functionType);
                insertInverterStatement.setBoolean(6, inverter.temperature);
                insertInverterStatement.addBatch();
                insertInverterStatement.executeBatch();

                //Add values
                PreparedStatement insertDataStatement = connection.prepareStatement(createInsertQuery(inverter, ignoreFlag), Statement.RETURN_GENERATED_KEYS);
                dateMapMap.forEach((date, values) -> {
                    try {
                        insertDataStatement.setTimestamp(1, new Timestamp(date.getTime()));
                        switch (inverter.function) {
                            case Wechselrichter:
                                insertDataStatement.setInt(2, values.get("PAC"));
                                insertDataStatement.setInt(inverter.strings + 3, values.get("YieldDay"));
                                for (int i=0; i< inverter.strings; i++) {
                                    insertDataStatement.setInt(3 + i, values.get("PDC" + (i + 1)));
                                    insertDataStatement.setInt(inverter.strings + 4 + i, values.get("UDC" + (i + 1)));
                                }
                                if (inverter.temperature) {
                                    insertDataStatement.setInt((inverter.strings * 2) + 5, values.get("Temperature"));
                                }
                                break;
                            case SensorBox:
                                insertDataStatement.setInt(2, values.get("SunExposure"));
                                insertDataStatement.setInt(3, values.get("ModuleTemperature"));
                                insertDataStatement.setInt(4, values.get("OutdoorTemperature"));
                                insertDataStatement.setInt(5, values.get("WindSpeed"));
                                break;
                            case SO_Stromzaehler:
                                if (inverter.functionType == 0) {
                                    insertDataStatement.setInt(2, values.get("TotalPAC"));
                                    insertDataStatement.setInt(inverter.strings + 3, values.get("TotalYieldDay"));
                                    for (int i=0; i< inverter.strings; i++) {
                                        insertDataStatement.setInt(3 + i, values.get("TotalPDC" + (i + 1)));
                                        insertDataStatement.setInt(inverter.strings + 4 + i, values.get("TotalUDC" + (i + 1)));
                                    }
                                } else {
                                    insertDataStatement.setInt(2, values.get("ConsPAC"));
                                    insertDataStatement.setInt(inverter.strings + 3, values.get("ConsYieldDay"));
                                    for (int i=0; i< inverter.strings; i++) {
                                        insertDataStatement.setInt(3 + i, values.get("ConsPDC" + (i + 1)));
                                        insertDataStatement.setInt(inverter.strings + 4 + i, values.get("ConsUDC" + (i + 1)));
                                    }
                                }
                                if (inverter.temperature) {
                                    insertDataStatement.setInt((inverter.strings * 2) + 5, values.get("Temperature"));
                                }
                                break;
                        }
                        insertDataStatement.addBatch();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
                insertDataStatement.executeBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });

        connection.commit();
    }

    private static String sanitizeInverterIdentifier(Inverter inverter) {
        return inverter.identifier.replaceAll("\"", "").replaceAll(" ", "_");
    }

    private static String createTableQuery(Inverter inverter) {
        StringBuilder builder = new StringBuilder()
                .append("CREATE TABLE IF NOT EXISTS `")
                .append(sanitizeInverterIdentifier(inverter))
                .append("` (" +
                        "timestamp TIMESTAMP NOT NULL," +
                        "PRIMARY KEY (timestamp),");
        switch (inverter.function) {
            case Wechselrichter:
                builder
                        .append("PAC INT,")
                        .append("YieldDay INT,");
                for (int i = 0; i < inverter.strings; i++) {
                    builder
                            .append("PDC").append(i + 1).append(" INT,")
                            .append("UDC").append(i + 1).append(" INT,");
                }
                if(inverter.temperature) {
                    builder.append("Temperature INT,");
                }
                break;
            case SensorBox:
                builder
                        .append("SunExposure INT,")
                        .append("ModuleTemperature INT,")
                        .append("OutdoorTemperature INT,")
                        .append("WindSpeed INT,");
                break;
            case SO_Stromzaehler:
                if(inverter.functionType == 0) {
                    builder
                            .append("TotalPAC INT,")
                            .append("TotalYieldDay INT,");
                    for (int i = 0; i < inverter.strings; i++) {
                        builder
                                .append("TotalPDC").append(i + 1).append(" INT,")
                                .append("TotalUDC").append(i + 1).append(" INT,");
                    }
                } else {
                    builder
                            .append("ConsPAC INT,")
                            .append("ConsYieldDay INT,");
                    for (int i = 0; i < inverter.strings; i++) {
                        builder
                                .append("ConsPDC").append(i + 1).append(" INT,")
                                .append("ConsUDC").append(i + 1).append(" INT,");
                    }
                }
                if(inverter.temperature) {
                    builder.append("Temperature INT,");
                }
                break;
        }
        //                                                 regex to eliminate last ","
        return builder.append(")").toString().replaceFirst("(?s)(.*),", "$1 ");
    }

    private static String createInsertQuery(Inverter inverter, boolean ignoreFlag) {
        StringBuilder builder = new StringBuilder()
                .append("INSERT ");
        if (ignoreFlag) {
            builder.append("IGNORE ");
        }
        builder
                .append("INTO `")
                .append(sanitizeInverterIdentifier(inverter))
                .append("` (timestamp, ");
        int values = 0;
        switch (inverter.function) {
            case Wechselrichter:
                builder
                        .append("PAC, ")
                        .append("YieldDay, ");
                values = 2;
                for (int i = 0; i < inverter.strings; i++) {
                    builder
                            .append("PDC").append(i + 1).append(", ")
                            .append("UDC").append(i + 1).append(", ");
                    values = values + 2;
                }
                if(inverter.temperature) {
                    builder.append("Temperature, ");
                    values++;
                }
                break;
            case SensorBox:
                builder
                        .append("SunExposure, ")
                        .append("ModuleTemperature, ")
                        .append("OutdoorTemperature, ")
                        .append("WindSpeed, ");
                values = 4;
                break;
            case SO_Stromzaehler:
                if(inverter.functionType == 0) {
                    builder
                            .append("TotalPAC, ")
                            .append("TotalYieldDay, ");
                    values = 2;
                    for (int i = 0; i < inverter.strings; i++) {
                        builder
                                .append("TotalPDC").append(i + 1).append(", ")
                                .append("TotalUDC").append(i + 1).append(", ");
                        values = values + 2;
                    }
                } else {
                    builder
                            .append("ConsPAC, ")
                            .append("ConsYieldDay, ");
                    values = 2;
                    for (int i = 0; i < inverter.strings; i++) {
                        builder
                                .append("ConsPDC").append(i + 1).append(", ")
                                .append("ConsUDC").append(i + 1).append(", ");
                        values = values + 2;
                    }
                }
                if(inverter.temperature) {
                    builder.append("Temperature, ");
                    values++;
                }
                break;
        }
        builder = new StringBuilder(builder.toString().replaceFirst("(?s)(.*), ", "$1 "));
        builder.append(") VALUES (?");
        for (int i = 0; i<values; i++) {
            builder.append(", ?");
        }
        builder.append(")");

        return builder.toString();
    }
}
