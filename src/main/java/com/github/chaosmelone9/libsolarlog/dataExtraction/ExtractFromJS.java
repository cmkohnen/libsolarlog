package com.github.chaosmelone9.libsolarlog.dataExtraction;

import com.github.chaosmelone9.libsolarlog.Inverter;
import com.github.chaosmelone9.libsolarlog.UnsupportedInverterFunctionException;
import com.github.chaosmelone9.libsolarlog.fileInteraction.GetFileContent;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Extracting data from .js files found on ftp-backups
 * @author Christoph Kohnen
 * @since 0.0.0rc0.2-0
 */
public class ExtractFromJS {
    private static final String DATEFORMAT = "dd.MM.yy HH:mm:ss";

    public static Map<Inverter, Map<Date, Map<String, Integer>>> extractDataFromJSFile(List<Inverter> inverters, File file) throws IOException, ParseException, UnsupportedInverterFunctionException, NullPointerException, NumberFormatException {
        Map<Inverter, Map<Date, Map<String, Integer>>> finalData = new HashMap<>();
        // loop for every inverter
        for (int i = 0; i < inverters.size(); i++) {
            Inverter inverter = inverters.get(i);
            Map<Date, Map<String, Integer>> inverterMap = new HashMap<>();
            // loop over every line in one file and extract values
            for (String line : GetFileContent.getFileContentAsList(file)) {
                // Get everything in quotation marks split by "|"
                List<String> separated = Arrays.asList(StringUtils.substringBetween(line, "\"", "\"").split("\\|"));
                // get the timestamp as Date
                Date date = new SimpleDateFormat(DATEFORMAT).parse(separated.get(0));
                Map<String, Integer> values = new HashMap<>();
                int inverterStrings = inverter.strings;
                if (inverterStrings == 0) inverterStrings = 1;
                // Get values as String for current inverter
                List<String> strings = Arrays.asList(separated.get(i + 1).split(";"));
                switch (inverter.function) {
                    case Wechselrichter:
                        //global pac, Index 0
                        values.put("PAC", Integer.valueOf(strings.get(0)));
                        // PDCs, Index string + 1
                        for(int j = 0; j < inverterStrings; j++) {
                            values.put(String.format("PDC%s", (j + 1)), Integer.valueOf(strings.get(j + 1)));
                        }
                        // Yield for the day, Index amount of strings + 1
                        values.put("YieldDay", Integer.parseInt(strings.get(inverterStrings + 1)));
                        // UDCs, Index amount of strings + 2 + string
                        for(int j = 0; j < inverterStrings; j++) {
                            values.put(String.format("UDC%s", (j + 1)), Integer.valueOf(strings.get(inverterStrings) + 2 + j));
                        }
                        if (inverter.temperature) {
                            // Temperature, Index amount of strings * 2 + 2
                            values.put("Temperature", Integer.valueOf(strings.get(inverterStrings*2 + 2)));
                        }
                        break;
                    case SensorBox:
                        values.put("SunExposure", Integer.valueOf(strings.get(0)));
                        values.put("ModuleTemperature", Integer.valueOf(strings.get(1)));
                        values.put("OutdoorTemperature", Integer.valueOf(strings.get(2)));
                        values.put("WindSpeed", Integer.valueOf(strings.get(3)));
                        break;
                    case SO_Stromzaehler:
                        if (inverter.functionType == 0) {
                            //total PAC, Index 0
                            values.put("TotalPAC", Integer.valueOf(strings.get(0)));
                            // PDCs, Index string + 1
                            for(int j = 0; j < inverterStrings; j++) {
                                values.put(String.format("TotalPDC%s", (j + 1)), Integer.valueOf(strings.get(j + 1)));
                            }
                            // Yield for the day, Index amount of strings + 1
                            values.put("TotalYieldDay", Integer.parseInt(strings.get(inverterStrings + 1)));
                            // UDCs, Index amount of strings + 2 + string
                            for(int j = 0; j < inverterStrings; j++) {
                                values.put(String.format("TotalUDC%s", (j + 1)), Integer.valueOf(strings.get(inverterStrings) + 2 + j));
                            }
                        } else {
                            //total ConsPAC, Index 0
                            values.put("ConsPAC", Integer.valueOf(strings.get(0)));
                            // PDCs, Index string + 1
                            for(int j = 0; j < inverterStrings; j++) {
                                values.put(String.format("ConsPDC%s", (j + 1)), Integer.valueOf(strings.get(j + 1)));
                            }
                            // Yield for the day, Index amount of strings + 1
                            values.put("ConsYieldDay", Integer.parseInt(strings.get(inverterStrings + 1)));
                            // UDCs, Index amount of strings + 2 + string
                            for(int j = 0; j < inverterStrings; j++) {
                                values.put(String.format("ConsUDC%s", (j + 1)), Integer.valueOf(strings.get(inverterStrings) + 2 + j));
                            }
                        }
                        if (inverter.temperature) {
                            // Temperature, Index amount of strings * 2 + 2
                            values.put("Temperature", Integer.valueOf(strings.get(inverterStrings*2 + 2)));
                        }
                        break;
                    default:
                        throw new UnsupportedInverterFunctionException(inverter.function.toString());
                }
                inverterMap.put(date, values);
            }
            finalData.put(inverter, inverterMap);
        }
        return finalData;
    }
}
