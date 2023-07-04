package io.opencurve.curve.fs;

public class Bucket {
    public String type;
    public String name;

    public Bucket(String type, String name) {
        this.type = type;
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String toString() {
        return "bucket[" + type + "," + name + "]";
    }
}
