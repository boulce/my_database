package org.example.record;

public class NullConst {
    public static int NULL_LINK = 0;

    public static boolean isNullAttribute(char[] attribute) {
        for (char c : attribute) {
            if(c != 0) {
                return false;
            }
        }
        return true;
    }
}
