package com.eh.memory;

/**
 * Profile publishing via track1 eventhubs library in fire&forget mode.
 */
public class T1FireForget {
    public static void main(String[] args) {
        ProfileThePublisher.main(new String[] {"1000", "60", "t1", "f"});
    }
}
