package org.example;

import org.example.metadata.AttributeMetadata;
import org.example.metadata.RelationMetadata;
import org.example.record.Block;
import org.example.ui.TextUI;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import java.sql.*;
import java.util.*;

public class Main {
    public static void main(String[] args) throws SQLException {
        TextUI textUI = new TextUI();
        textUI.run();
    }
}