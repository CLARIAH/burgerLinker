package nl.knaw.iisg.burgerlinker.utilities;

import java.lang.Thread;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ActivityIndicator extends Thread {
    char[] symbols = {'|', '/', '-', '\\', '|', '/', '-', '\\'};
    int interval, interval_default = 500;  // ms
    String pre = "", post = "";
    public boolean active = false;

    static final Logger lg = LogManager.getLogger(ActivityIndicator.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);

    public ActivityIndicator() {
        this.interval = interval_default;
    }

    public ActivityIndicator(String pre) {
        this.pre = pre;
        this.interval = interval_default;
    }

    public ActivityIndicator(String pre, String post) {
        this.pre = pre;
        this.post = post;
        this.interval = interval_default;
    }

    public ActivityIndicator(String pre, int interval) {
        this.pre = pre;

        this.interval = interval;
        if (this.interval <= 0) {
            this.interval = interval_default;
        }
    }

    public ActivityIndicator(String pre, String post, int interval) {
        this.pre = pre;
        this.post = post;

        this.interval = interval;
        if (this.interval <= 0) {
            this.interval = interval_default;
        }
    }

    public void run() {
        this.active = true;

        int i = 0;
        String pre = (this.pre.length() > 0) ? this.pre + " " : this.pre;
        String post = (this.post.length() > 0) ? " " + this.post : this.post;

        System.out.print(pre + this.symbols[i++] + post);

        int dynLen;
        while (active) {
            dynLen = post.length() + 1;
            post = (this.post.length() > 0) ? " " + this.post : this.post;

            System.out.print("\b".repeat(dynLen) + this.symbols[i++] + post);

            try {
                Thread.sleep(this.interval);
            } catch (Exception e) {
                LOG.logError("run", "Error with sleeping thread: " + e);
            };
            if (i >= this.symbols.length) {
                i = 0;
            }
        }

        dynLen = post.length() + 1;
        System.out.println("\b".repeat(dynLen) + "OK");
        System.out.flush();
    }

    public void terminate() {
        this.active = false;
    }

    public void update(String post) {
        this.post = post;
    }

    public void update(int post) {
        this.post = String.valueOf(post);
    }
}
