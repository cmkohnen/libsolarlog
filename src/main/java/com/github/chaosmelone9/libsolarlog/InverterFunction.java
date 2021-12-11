package com.github.chaosmelone9.libsolarlog;

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
