package io.opencurve.curve.fs.others;

import junit.framework.TestCase;
import io.opencurve.curve.fs.hadoop.CurveFileSystem;

import java.io.IOException;
import java.net.URL;

public class TestOthers extends TestCase {
    public void testHelloWorld() throws IOException {
        URL location = CurveFileSystem.class.getProtectionDomain().getCodeSource().getLocation();
        System.out.println("Hello World");
        System.out.println(location.getPath());
    }
}
