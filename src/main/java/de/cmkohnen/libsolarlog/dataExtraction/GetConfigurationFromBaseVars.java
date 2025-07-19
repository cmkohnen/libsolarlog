package de.cmkohnen.libsolarlog.dataExtraction;

import de.cmkohnen.libsolarlog.Inverter;
import de.cmkohnen.libsolarlog.InverterFunction;
import de.cmkohnen.libsolarlog.fileInteraction.GetFileContent;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Extracting configuration of SolarLog installation from "base_vars.js"
 * @author Christoph Kohnen
 * @since 0.0.0rc0.2-0
 */
public class GetConfigurationFromBaseVars {
    public static List<Inverter> getInvertersFromBaseVars(File base_vars) throws IOException {
        List<Inverter> inverters = new ArrayList<>();
        for (String line : GetFileContent.getFileContentAsList(base_vars)) {
            if (line.startsWith("WRInfo") && !(StringUtils.substringBetween(line, "(" , ")") == null)) {
                try {
                    List<String> values = Arrays.asList(StringUtils.substringBetween(line, "(", ")").split(","));
                    String type = values.get(0);
                    String identifier = values.get(4);
                    int strings = Integer.parseInt(values.get(5));
                    InverterFunction function = InverterFunction.fromOrdinal(Integer.parseInt(values.get(11)));
                    int functionType = 0;
                    if(function == InverterFunction.SO_Stromzaehler) {
                        functionType = Integer.parseInt(values.get(values.size() - 1));
                    }
                    boolean temperature = values.get(12).equals("1");
                    inverters.add(new Inverter(type, identifier, strings, function, functionType, temperature));
                } catch (ArrayIndexOutOfBoundsException ignored) {
                    //Code will get here by multiple strings ("WRInfo[1][6]=new Array("String 1","String 2")"), can be ignored
                }
            }
        }
        return inverters;
    }
}
