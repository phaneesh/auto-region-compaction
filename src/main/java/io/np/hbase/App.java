/*
 * Copyright 2016 Phaneesh Nagaraja <phaneesh.n@gmail.com>.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package io.np.hbase;

import com.github.seratch.jslack.Slack;
import com.github.seratch.jslack.api.methods.request.chat.ChatPostMessageRequest;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Hello world!
 */
public class App {

    private static final Logger logger = LoggerFactory.getLogger("HBaseAutoRegionCompactor");

    private static final DecimalFormat df = new DecimalFormat("##.00");

    public static void main(String[] args) throws Exception {
        CommandLineParser commandLineParser = new DefaultParser();
        Option zookeeperHosts = Option.builder()
                .argName("zookeeperHosts")
                .longOpt("zookeeperHosts")
                .hasArg(true)
                .desc("Comma separated zookeeper host list")
                .required()
                .type(String.class)
                .valueSeparator(',')
                .build();

        Option zookeeperPort = Option.builder()
                .argName("zookeeperPort")
                .longOpt("zookeeperPort")
                .hasArg(true)
                .desc("Zookeeper Port")
                .type(String.class)
                .build();

        Option hbaseZnode = Option.builder()
                .argName("hbaseZNode")
                .longOpt("hbaseZNode")
                .hasArg(true)
                .desc("ZNode for hbase")
                .type(String.class)
                .build();

        Option tables = Option.builder()
                .argName("tables")
                .longOpt("tables")
                .hasArg(true)
                .desc("Comma separated list of tables that needs to be monitored")
                .type(String.class)
                .valueSeparator(',')
                .build();

        Option localistThreshold = Option.builder()
                .argName("localityThreshold")
                .longOpt("localityThreshold")
                .hasArg(true)
                .desc("Data Locality threshold")
                .type(Double.class)
                .build();

        Option slackAlert = Option.builder()
                .argName("slackAlert")
                .longOpt("slackAlert")
                .hasArg(true)
                .desc("Enable Slack alert")
                .type(String.class)
                .build();

        Option slackToken = Option.builder()
                .argName("slackToken")
                .longOpt("slackToken")
                .hasArg(true)
                .desc("Slack token")
                .type(String.class)
                .build();

        Option slackChannel = Option.builder()
                .argName("slackChannel")
                .longOpt("slackChannel")
                .hasArg(true)
                .desc("Slack channel")
                .type(String.class)
                .build();

        Option slackUserName = Option.builder()
                .argName("slackUserName")
                .longOpt("slackUserName")
                .hasArg(true)
                .desc("Slack user name")
                .type(String.class)
                .build();

        Options options = new Options();
        options.addOption(zookeeperHosts);
        options.addOption(zookeeperPort);
        options.addOption(hbaseZnode);
        options.addOption(localistThreshold);
        options.addOption(tables);
        options.addOption(slackAlert);
        options.addOption(slackToken);
        options.addOption(slackChannel);
        options.addOption(slackUserName);

        CommandLine commandLine = commandLineParser.parse(options, args);

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", commandLine.getOptionValue("zookeeperPort", "2181"));
        conf.set("hbase.zookeeper.quorum", commandLine.getOptionValue("zookeeperHosts"));
        conf.set("zookeeper.znode.parent", commandLine.getOptionValue("hbaseZNode", "/hbase-unsecure"));
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        ClusterStatus status = admin.getClusterStatus();
        StringBuilder builder = new StringBuilder();
        builder.append("----------------------------------------\n");
        builder.append("HBase Region Compaction Alert:\n");
        builder.append("----------------------------------------\n");
        List<String> selectedTables = commandLine.hasOption("tables") ? Arrays
                .asList(commandLine.getOptionValues("tables")) : Arrays.stream(admin.listTableNames())
                .filter(n -> !n.isSystemTable())
                .map(TableName::getNameAsString).collect(Collectors.toList());
        logger.info("Total Tables: " + selectedTables.size());
        double locality = Double.parseDouble(commandLine.getOptionValue("localityThreshold", "0.99"));
        for (ServerName server : status.getServers()) {
            ServerLoad load = status.getLoad(server);
            for (Map.Entry<byte[], RegionLoad> entry :
                    load.getRegionsLoad().entrySet()) {
                RegionLoad regionLoad = entry.getValue();
                if (selectedTables.contains(HRegionInfo.getTable(regionLoad.getName()).getNameAsString())) {
                    logger.info("Name: " + regionLoad.getNameAsString() + " | Data Locality: "
                            + regionLoad.getDataLocality() + "| Region Server: " + server.getHostname());
                    if (regionLoad.getDataLocality() < locality && regionLoad.getStorefileSizeMB() != 0) {
                        builder.append("Data Locality: ").append(regionLoad.getDataLocality()).append('\n');
                        builder.append("Region Server: ").append(server.getHostname()).append('\n');
                        builder.append("Name: ").append(regionLoad.getNameAsString()).append('\n');
                        if (regionLoad.getTotalCompactingKVs() != regionLoad.getCurrentCompactedKVs()) {
                            BigDecimal remainingKeys = BigDecimal.valueOf(regionLoad.getTotalCompactingKVs())
                                    .subtract(BigDecimal.valueOf(regionLoad.getCurrentCompactedKVs()));
                            BigDecimal progress = remainingKeys.multiply(new BigDecimal("100"))
                                    .divide(BigDecimal.valueOf(regionLoad.getTotalCompactingKVs()),
                                            BigDecimal.ROUND_HALF_UP);
                            builder.append("Compaction Progress: ").append(df.format(progress)).append('%').append('\n');
                        } else {
                            try {
                                admin.majorCompactRegion(regionLoad.getName());
                            } catch (Exception e) {
                                logger.error("Error running major compaction on region: {}", regionLoad.getNameAsString());
                            }
                        }
                    }
                }
            }
        }
        builder.append("----------------------------------------\n");
        if (Boolean.parseBoolean(commandLine.getOptionValue("slackAlert", "false"))) {
            final Slack slack = Slack.getInstance();
            slack.methods().chatPostMessage(
                    ChatPostMessageRequest.builder()
                            .token(commandLine.getOptionValue("slackToken"))
                            .channel(commandLine.getOptionValue("slackChannel"))
                            .text(builder.toString())
                            .iconEmoji(":robot_face:")
                            .username(commandLine.getOptionValue("slackUserName", "Auto Region Compaction"))
                            .build());
        }
        logger.info(builder.toString());

    }
}
