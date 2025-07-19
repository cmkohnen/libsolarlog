package de.cmkohnen.libsolarlog;

/**
 * @author Christoph Kohnen
 * @since 0.0.0rc0.4-0
 */
public class UnsupportedInverterFunctionException extends Exception {
    public UnsupportedInverterFunctionException(String errorMessage) {
        super(errorMessage);
    }
}
