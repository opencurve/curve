package io.opencurve.curve.fs;

import java.util.Arrays;

public class CurveFileExtent {
    public long offset;
    public long length;
    public int[] osds;

    CurveFileExtent(long offset, long length, int[] osds) {
        this.offset = offset;
        this.length = length;
        this.osds = osds;
    }

    /**
     * Get starting offset of extent.
     */
    public long getOffset() {
        return offset;
    }

    /**
     * Get length of extent.
     */
    public long getLength() {
        return length;
    }

    /**
     * Get list of OSDs with this extent.
     */
    public int[] getOSDs() {
        return osds;
    }

    /**
     * Pretty print.
     */
    public String toString() {
        return "extent[" + offset + "," + length + ","
                + Arrays.toString(osds) + "]";
    }
}
