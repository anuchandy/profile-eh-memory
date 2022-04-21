package com.eh.memory;

/**
 * Profile publishing via track2 eventhubs library in coordinated mode.
 */
public class T2Coordinated {
    public static void main(String[] args) {
        ProfileThePublisher.main(new String[] {"1000", "60", "t2", "c"});
    }
}
