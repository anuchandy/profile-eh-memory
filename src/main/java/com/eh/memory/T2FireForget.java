package com.eh.memory;

/**
 * Profile publishing via track2 eventhubs library in fire&forget mode.
 */
public class T2FireForget {
    public static void main(String[] args) {
        ProfileThePublisher.main(new String[] {"1000", "60", "t2", "f"});
    }
}
