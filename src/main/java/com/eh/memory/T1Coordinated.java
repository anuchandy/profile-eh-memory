package com.eh.memory;

/**
 * Profile publishing via track1 eventhubs library in coordinated mode.
 */
public class T1Coordinated {
    public static void main(String[] args) {
        ProfileThePublisher.main(new String[] {"1000", "60", "t1", "c"});
    }
}
