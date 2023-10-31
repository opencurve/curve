package io.opencurve.curve.fs.common;

public class AccessLogger {
    private String classname;
    private int level;

    private static final String CURVEFS_DEBUG_ENV_VAR = "CURVEFS_DEBUG";

    public AccessLogger(String classname, int level) {
        this.classname = classname;
        this.level = level;
    }

    public void log(String fn, Object... args) {
        String value = System.getenv(CURVEFS_DEBUG_ENV_VAR);
        if (!Boolean.valueOf(value)) {
            return;
        }

        String[] params = new String[args.length];
        for (int i = 0; i < args.length; i++) {
            params[i] = args[i].toString();
        }

        String indent = " ".repeat(this.level * 4);
        String param = String.join(",", params);
        String message = String.format("+ %s%s.%s(%s)", indent, this.classname, fn, param);
        System.out.println(message);
    }
}
