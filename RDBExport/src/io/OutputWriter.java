package io;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import models.KeyInformation;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class OutputWriter {

    private static final ReentrantLock reentrantLock = new ReentrantLock();

    private static ICSVWriter writer = null;

    public static void buildOutputWriter(String path) {
        try {
            createFileIfNecessary(path);
            writer = new CSVWriterBuilder(new FileWriter(path)).build();
            writeHeader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void writeNext(KeyInformation keyInformation) throws IOException {
        String[] line = getLine(keyInformation);

        reentrantLock.lock();
        try {
            writer.writeNext(line);
            writer.flush();
        } finally {
            reentrantLock.unlock();
        }
    }

    public static void writeAll(List<KeyInformation> keyInformations) throws IOException {
        List<String[]> allLines = new ArrayList<>();
        for (KeyInformation keyInformation : keyInformations) {
            allLines.add(getLine(keyInformation));
        }

        writer.writeAll(allLines);
        writer.flush();
    }

    private static String[] getLine(KeyInformation keyInformation) {
        return new String[] {
                keyInformation.getType().name().toLowerCase(),
                keyInformation.getKeyspace(),
                String.valueOf(keyInformation.getTenantId()),
                keyInformation.getKey(),
                String.valueOf(keyInformation.getSizeInBytes()),
                String.valueOf(keyInformation.getNumElements()),
                keyInformation.getClientProvider().name(),
                keyInformation.getFormattedExpireDatetime()
        };
    }

    private static void createFileIfNecessary(String path) throws IOException {
        File file = new File(path);
        if (!file.exists()) {
            file.createNewFile();
        }
    }

    private static void writeHeader() throws IOException {
        String[] headers = {"type", "keyspace", "tenant_id", "key", "size_in_bytes", "num_elements", "client_provider", "expire_datetime"};
        writer.writeNext(headers);
        writer.flush();
    }
}
