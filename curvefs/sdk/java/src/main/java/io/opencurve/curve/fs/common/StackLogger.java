package io.opencurve.curve.fs.common;

import java.text.SimpleDateFormat;
import java.util.Date;

public class StackLogger {
    private String classname;
    private int level;

    private static final String CURVEFS_DEBUG_ENV_VAR = "CURVEFS_DEBUG";

    public StackLogger(String classname, int level) {
        this.classname = classname;
        this.level = level;
    }

    private String now() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return formatter.format(new Date());
    }

    private int getIndex() {
        long id = Thread.currentThread().getId();
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
        String message = String.format("+ [%s] %s%s.%s(%s)", now(), indent, this.classname, fn, param);
        System.out.println(message);
    }
}
