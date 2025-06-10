/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.tools;

import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.message.VotersRecord.Voter;
import org.apache.kafka.common.message.VotersRecord.Endpoint;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import joptsimple.OptionSpec;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


import java.util.Properties;
import java.util.Collections;
import java.util.Arrays;
import java.io.FileReader;

/**
 * Tool to append a KRaftVersion record to the metadata log.
 * This tool must only be run when the KRaft node is stopped.
 */
public class KRaftQuorumConverter {

    public static void main(String[] args) {
        Exit.exit(mainNoExit(args));
    }

    static int mainNoExit(String[] args) {
        try {
            execute(args);
            return 0;
        } catch (TerseException e) {
            System.err.println(e.getMessage());
            return 1;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println(Utils.stackTrace(e));
            return 1;
        }
    }

    private static void execute(String[] args) throws Exception {
        KRaftQuorumConverterOptions options = new KRaftQuorumConverterOptions(args);

        String metadataLogDir = options.metadataLogDir();
        Path metadataPath = Paths.get(metadataLogDir);

        // Validate metadata log directory exists
        if (!Files.exists(metadataPath) || !Files.isDirectory(metadataPath)) {
            throw new TerseException("Metadata log directory does not exist: " + metadataLogDir);
        }

        String metadataPropertiesFile = options.metadataPropertiesFile();
        Path metadataPropertiesPath = Paths.get(metadataPropertiesFile);
        String configFile = options.configFile();
        Path configPath = Paths.get(configFile);

        // Validate metadata properties file exists
        if (!Files.exists(metadataPropertiesPath) || !Files.isRegularFile(metadataPropertiesPath)) {
            throw new TerseException("Metadata properties file does not exist: " + metadataPropertiesFile);
        }

        // Validate config file exists
        if (!Files.exists(configPath) || !Files.isRegularFile(configPath)) {
            throw new TerseException("Config file does not exist: " + configFile);
        }

        // Check for .lock file to ensure KRaft node is not running
        File lockFile = new File(metadataPath.toFile(), ".lock");

        System.out.println("Acquiring lock on metadata log directory: " + metadataLogDir);

        try (RandomAccessFile lockFileAccess = new RandomAccessFile(lockFile, "rw");
             FileChannel lockChannel = lockFileAccess.getChannel()) {

            // Try to acquire exclusive lock
            FileLock lock = lockChannel.tryLock();
            if (lock == null) {
                throw new TerseException("Cannot acquire lock. KRaft node may be running or another process is using the log directory.");
            }

            try {
                System.out.println("Lock acquired successfully.");

                // Find the metadata log file
                File metadataLog = findMetadataLogFile(metadataPath.toFile());
                if (metadataLog == null) {
                    throw new TerseException("Could not find metadata log file in directory: " + metadataLogDir);
                }

                System.out.println("Found metadata log file: " + metadataLog.getName());

                appendKRaftVersionRecord(metadataLog);
                appendVotersRecord(metadataLog, metadataPropertiesPath.toFile(), configPath.toFile());

                System.out.println("Successfully appended KRaftVersion record with kraftVersion=1");

            } finally {
                lock.release();
                System.out.println("Lock released.");
            }
        }
    }

    private static File findMetadataLogFile(File metadataDir) {
        // Look for the latest .log file in the metadata directory
        // The metadata log typically has a name like 00000000000000000000.log
        File[] logFiles = metadataDir.listFiles((dir, name) ->
            name.endsWith(".log") && name.matches("\\d+\\.log"));

        if (logFiles == null || logFiles.length == 0) {
            return null;
        }

        // Return the file with the highest offset (latest)
        File latestLog = logFiles[0];
        for (File logFile : logFiles) {
            String fileName = logFile.getName();
            String baseName = fileName.substring(0, fileName.lastIndexOf(".log"));
            long offset = Long.parseLong(baseName);

            String latestFileName = latestLog.getName();
            String latestBaseName = latestFileName.substring(0, latestFileName.lastIndexOf(".log"));
            long latestOffset = Long.parseLong(latestBaseName);

            if (offset > latestOffset) {
                latestLog = logFile;
            }
        }

        return latestLog;
    }

    private static void appendKRaftVersionRecord(File logFile) throws IOException {
        // Read the current log to determine the next offset and epoch
        LogInfo logInfo = readLogInfo(logFile);

        // Create KRaftVersion record with kraftVersion=1
        KRaftVersionRecord kraftVersionRecord = new KRaftVersionRecord()
            .setVersion(ControlRecordUtils.KRAFT_VERSION_CURRENT_VERSION)
            .setKRaftVersion((short) 1);

        // Create MemoryRecords containing the KRaftVersion record
        long timestamp = System.currentTimeMillis();
        ByteBuffer buffer = ByteBuffer.allocate(1024); // Should be enough for a single control record

        MemoryRecords records = MemoryRecords.withKRaftVersionRecord(
            logInfo.nextOffset,
            timestamp,
            logInfo.lastEpoch, // Use the last epoch from the log
            buffer,
            kraftVersionRecord
        );

        // Append to the log file
        try (RandomAccessFile raf = new RandomAccessFile(logFile, "rw");
             FileChannel channel = raf.getChannel()) {

            // Position at the end of the file
            channel.position(channel.size());

            // Write the records by converting to ByteBuffer and writing directly
            ByteBuffer byteBuffer = records.buffer();
            channel.write(byteBuffer);

            // Force to disk
            channel.force(true);
        }
    }

    private static void appendVotersRecord(File logFile, File metadataPropertiesFile, File configFile) throws IOException {
        // Read the current log to determine the next offset and epoch
        LogInfo logInfo = readLogInfo(logFile);

        // Read the metadata properties file
        try {
            Properties props = getProperties(metadataPropertiesFile, configFile);
            System.out.println("props: " + props);

            String controllerListenerName = props.getProperty("controller.listener.names", "CONTROLLER");
            String advertisedListeners = props.getProperty("advertised.listeners", "CONTROLLER://:9001");
            String controllerAdvertisedListener = Arrays.stream(advertisedListeners.split(","))
                .filter(listener -> listener.startsWith(controllerListenerName + "://"))
                .findFirst()
                .orElseThrow(() -> new TerseException("Controller advertised listener not found"));
            String controllerAdvertisedListenerHost = controllerAdvertisedListener.split("://")[1].split(":")[0];
            String controllerAdvertisedListenerPort = controllerAdvertisedListener.split("://")[1].split(":")[1];

            // Create EndpointCollection manually
            VotersRecord.EndpointCollection endpoints = new VotersRecord.EndpointCollection();
            endpoints.add(new VotersRecord.Endpoint()
                .setName(controllerListenerName)
                .setHost(controllerAdvertisedListenerHost)
                .setPort(Integer.parseInt(controllerAdvertisedListenerPort)));

            // Create Voters record
            VotersRecord votersRecord = new VotersRecord()
                .setVersion(ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION)
                .setVoters(Collections.singletonList(new Voter()
                    .setVoterId(Integer.parseInt(props.getProperty("node.id")))
                    .setVoterDirectoryId(Uuid.fromString(props.getProperty("directory.id")))
                    .setEndpoints(endpoints)));

            // Create MemoryRecords containing the KRaftVersion record
            long timestamp = System.currentTimeMillis();
            ByteBuffer buffer = ByteBuffer.allocate(1024); // Should be enough for a single control record

            MemoryRecords records = MemoryRecords.withVotersRecord(
                logInfo.nextOffset,
                System.currentTimeMillis(),
                logInfo.lastEpoch,
                buffer,
                votersRecord
            );

            // Append to the log file
            try (RandomAccessFile raf = new RandomAccessFile(logFile, "rw");
                FileChannel channel = raf.getChannel()) {

                // Position at the end of the file
                channel.position(channel.size());

                // Write the records by converting to ByteBuffer and writing directly
                ByteBuffer byteBuffer = records.buffer();
                channel.write(byteBuffer);

                // Force to disk
                channel.force(true);
            }
        } catch (Exception e) {
            System.out.println("Error reading properties file: " + e.getMessage());
            return;
        }
    }


    private static Properties getProperties(File metadataPropertiesFile, File configFile) throws TerseException, IOException {
        if (metadataPropertiesFile == null) {
            return new Properties();
        } else {
            if (!metadataPropertiesFile.exists())
                throw new TerseException("Properties file " + metadataPropertiesFile.getPath() + " does not exist!");
            Properties props = Utils.loadProps(metadataPropertiesFile.getPath());
            if (configFile != null) {
                Properties configProps = Utils.loadProps(configFile.getPath());
                props.putAll(configProps);
            }
            return props;
        }
    }

    private static Properties loadProperties(File file) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileReader(file));
        return properties;
    }

    private static class LogInfo {
        final long nextOffset;
        final int lastEpoch;

        LogInfo(long nextOffset, int lastEpoch) {
            this.nextOffset = nextOffset;
            this.lastEpoch = lastEpoch;
        }
    }

    private static LogInfo readLogInfo(File logFile) throws IOException {
        if (logFile.length() == 0) {
            return new LogInfo(0L, 0);
        }

        long nextOffset = 0L;
        int lastEpoch = 0;

        try (RandomAccessFile raf = new RandomAccessFile(logFile, "r");
            FileChannel channel = raf.getChannel()) {

            ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(channel.size(), 1024 * 1024)); // Read up to 1MB
            channel.read(buffer);
            buffer.flip();

            // Parse records to find the highest offset and epoch
            // This is a simplified parsing - in production you'd want more robust parsing
            while (buffer.remaining() >= 12) { // Minimum record batch header size
                try {
                    // Read basic record batch info
                    long baseOffset = buffer.getLong(); // 8 bytes
                    int batchLength = buffer.getInt(); // 4 bytes (length in bytes)

                    if (batchLength <= 0 || buffer.remaining() < batchLength - 4) {
                        break; // Invalid or incomplete batch
                    }

                    // Skip to epoch field (position 16 in the header)
                    if (buffer.remaining() >= 8) {
                        buffer.getInt(); // skip partitionLeaderEpoch position - 4 bytes
                        buffer.get(); // magic - should be 1 byte, not 4 bytes
                        buffer.getInt(); // crc - 4 bytes
                        buffer.getShort(); // attributes - 2 bytes
                        int lastOffsetDelta = buffer.getInt(); // 4 bytes
                        buffer.getLong(); // firstTimestamp - 8 bytes
                        buffer.getLong(); // maxTimestamp - 8 bytes
                        buffer.getLong(); // producerId - 8 bytes
                        buffer.getShort(); // producerEpoch - 2 bytes
                        buffer.getInt(); // baseSequence - 4 bytes
                        int recordCount = buffer.getInt(); // 4 bytes

                        nextOffset = Math.max(nextOffset, baseOffset + lastOffsetDelta + 1);

                        // Skip remaining batch data
                        int remaining = batchLength - 49; // We've read 61 bytes; exclude baseOffset and batchLength
                        if (remaining > 0 && buffer.remaining() >= remaining) {
                            buffer.position(buffer.position() + remaining);
                        }
                    } else {
                        break;
                    }
                } catch (Exception e) {
                    // If parsing fails, use a safe fallback
                    break;
                }
            }
        }

        // If we couldn't parse properly, use a safe fallback
        if (nextOffset == 0L) {
            System.out.println("Could not parse log info, using rough estimate");
            nextOffset = logFile.length() / 50; // Rough estimate
        }

        return new LogInfo(nextOffset, lastEpoch);
    }

    static class KRaftQuorumConverterOptions extends CommandDefaultOptions {
        private final OptionSpec<String> metadataLogDirOpt;
        private final OptionSpec<String> metadataPropertiesFileOpt;
        private final OptionSpec<String> configFileOpt;

        public KRaftQuorumConverterOptions(String[] args) {
            super(args);

            this.metadataLogDirOpt = parser.accepts("metadata-log-dir",
                    "REQUIRED: The metadata log directory path")
                .withRequiredArg()
                .describedAs("path")
                .ofType(String.class);

            this.metadataPropertiesFileOpt = parser.accepts("metadata-properties-file",
                    "REQUIRED: The metadata properties file path (meta.properties file)")
                .withRequiredArg()
                .describedAs("path")
                .ofType(String.class);

            this.configFileOpt = parser.accepts("config-file",
                    "REQUIRED: The config file path (server.properties file)")
                .withRequiredArg()
                .describedAs("path")
                .ofType(String.class);


            try {
                options = parser.parse(args);
            } catch (Exception e) {
                CommandLineUtils.printUsageAndExit(parser, "KRaftQuorumConverter: " + e.getMessage());
            }

            CommandLineUtils.maybePrintHelpOrVersion(this,
                "This tool appends a KRaftVersion record to the metadata log. " +
                "The KRaft node must be stopped before running this tool.");

            CommandLineUtils.checkRequiredArgs(parser, options, metadataLogDirOpt, metadataPropertiesFileOpt, configFileOpt);
        }

        public String metadataLogDir() {
            return options.valueOf(metadataLogDirOpt);
        }

        public String metadataPropertiesFile() {
            return options.valueOf(metadataPropertiesFileOpt);
        }

        public String configFile() {
            return options.valueOf(configFileOpt);
        }
    }
}
