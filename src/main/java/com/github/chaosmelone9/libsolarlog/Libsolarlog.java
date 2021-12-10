package com.github.chaosmelone9.libsolarlog;

import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;

import java.io.FileReader;
import java.io.IOException;

/**
 * Main Class for the libsolarlog project
 * @author Christoph Kohnen
 * @since 0.0.0rc0.1-0
 */
public class Libsolarlog {

    private static Model projectModel() throws IOException, XmlPullParserException {
        MavenXpp3Reader reader = new MavenXpp3Reader();
        return reader.read(new FileReader("pom.xml"));
    }

    public static String getVersion() throws XmlPullParserException, IOException {
        return projectModel().getVersion();
    }

    public static String getURL() throws XmlPullParserException, IOException {
        return projectModel().getUrl();
    }
}
