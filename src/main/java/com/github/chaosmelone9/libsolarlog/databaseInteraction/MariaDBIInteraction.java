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
public class MariaDBIInteraction {

    /*
        // Connection Configuration
        Properties connConfig = new Properties();
        connConfig.setProperty("user", "db_user");
        connConfig.setProperty("password", "db_user_password");
        connConfig.setProperty("useSsl", "true");
        connConfig.setProperty("serverSslCert", "/path/to/ca-bundle.pem");

        String address = "jdbc:mariadb://192.0.2.1:3306";
     */

    public static String createTableQuery(Inverter inverter) {
        StringBuilder builder = new StringBuilder()
                .append("CREATE TABLE IF NOT EXISTS `")
                .append(inverter.identifier.replaceAll("\"", ""))
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
            case SensorBox:
                builder
                        .append("SunExposure INT,")
                        .append("ModuleTemperature INT,")
                        .append("OutdoorTemperature INT,")
                        .append("WindSpeed INT,");
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
                            .append("TotalPAC INT,")
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
        }
        //                                                 regex to eliminate last ","
        return builder.append(")").toString().replaceFirst("(?s)(.*),", "$1 ");
    }

    public static void pushToMariaDB(Properties connConfig, String address, String database, Map<Inverter, Map<Date, Map<String, Integer>>> data) throws SQLException {
        // Create Connection to MariaDB Enterprise
        Connection connection = DriverManager.getConnection(address, connConfig);

        // Disable Auto-Commit
        connection.setAutoCommit(false);

        // Use Statement to Create test.contacts table
        Statement statement = connection.createStatement();
        statement.execute("USE `" + database + "`");
        statement.execute("CREATE TABLE IF NOT EXISTS inverter(" +
                "inverter_identifier VARCHAR (50) NOT NULL," +
                "PRIMARY KEY (inverter_identifier)," +
                "inverter_type VARCHAR (50) NOT NULL," +
                "strings INT NOT NULL," +
                "function INT NOT NULL," +
                "function_type INT NOT NULL," +
                "temperature INT NOT NULL" +
                ")");

        data.forEach((inverter, dateMapMap) -> {
            try {
                /*
                statement.execute("CREATE TABLE IF NOT EXISTS `" + inverter.identifier.replaceAll("\"", "") + "` (" +
                        "timestamp TIMESTAMP NOT NULL," +
                        "PRIMARY KEY (timestamp)," +
                        "PAC INT," +
                        "PDC1 INT," +
                        "PDC2 INT," +
                        "PDC3 INT," +
                        "YieldDay INT," +
                        "UDC1 INT," +
                        "UDC2 INT," +
                        "UDC3 INT," +
                        "Temperature INT," +
                        "SunExposure INT," +
                        "ModuleTemperature INT," +
                        "OutdoorTemperature INT," +
                        "WindSpeed INT," +
                        "TotalPAC INT," +
                        "TotalPDC1 INT," +
                        "TotalPDC2 INT," +
                        "TotalPDC3 INT," +
                        "TotalYieldDay INT," +
                        "TotalUDC1 INT," +
                        "TotalUDC2 INT," +
                        "TotalUDC3 INT," +
                        "ConsPAC INT," +
                        "ConsPDC1 INT," +
                        "ConsPDC2 INT," +
                        "ConsPDC3 INT," +
                        "ConsYieldDay INT," +
                        "ConsUDC1 INT," +
                        "ConsUDC2 INT," +
                        "ConsUDC3 INT" +
                        ")");
                 */

                statement.execute(createTableQuery(inverter));

                PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO `" + inverter.identifier.replaceAll("\"", "") + "` (timestamp, PAC, PDC1, YieldDay, UDC1) VALUES (?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
                dateMapMap.forEach((date, stringIntegerMap) -> {
                    try {
                        preparedStatement.setTimestamp(1, new Timestamp(date.getTime()));
                        preparedStatement.setInt(2, stringIntegerMap.get("PAC"));
                        preparedStatement.setInt(3, stringIntegerMap.get("PDC1"));
                        //preparedStatement.setInt(4, stringIntegerMap.get("PDC2"));
                        //preparedStatement.setInt(5, stringIntegerMap.get("PDC3"));
                        preparedStatement.setInt(4, stringIntegerMap.get("YieldDay"));
                        preparedStatement.setInt(5, stringIntegerMap.get("UDC1"));
                        //preparedStatement.setInt(8, stringIntegerMap.get("UDC2"));
                        //preparedStatement.setInt(9, stringIntegerMap.get("UDC3"));
                        preparedStatement.addBatch();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
                preparedStatement.executeBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });

        // Commit Changes
        connection.commit();
    }
}
