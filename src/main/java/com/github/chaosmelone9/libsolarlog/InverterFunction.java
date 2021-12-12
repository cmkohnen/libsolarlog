package com.github.chaosmelone9.libsolarlog;

/**
 * Main Class for the libsolarlog project
 * @author Christoph Kohnen
 * @since 0.0.0rc0.2-0
 */
public enum InverterFunction {
    Wechselrichter,
    SensorBox,
    SO_Stromzaehler,
    Utility_Meter_1,
    Utility_Meter_2,
    Windsensor,
    Sub_Verbrauchszaehler;

    public static InverterFunction fromOrdinal(int ordinal) {return values()[ordinal];}
}
