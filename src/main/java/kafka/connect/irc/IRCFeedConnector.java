/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.connect.irc;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Generic Kafka Connector which takes a list of channels from an IRC server and creates 1 or more tasks depending
 * on maxTasks configuration argument.
 */

public class IRCFeedConnector extends SourceConnector {

    public static final String IRC_HOST_CONFIG = "irc.host";
    public static final String IRC_PORT_CONFIG = "irc.port";
    public static final String IRC_CHANNELS_CONFIG = "irc.channels";
    public static final String TOPIC_CONFIG = "topic";
    private String[] channels = null;
    private String topic = null;
    private String port = null;
    private String host = null;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        channels = props.get(IRC_CHANNELS_CONFIG).split(",");
        host = props.get(IRC_HOST_CONFIG);
        port = props.get(IRC_PORT_CONFIG);
        topic = props.get(TOPIC_CONFIG);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return IRCFeedTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        int numTasks = Math.min(maxTasks, channels.length);
        int channelsPerTask = Math.round(Float.valueOf(channels.length) / numTasks);
        for (int t = 0; t < numTasks; t++) {
            String channelSubset = "";
            for (int c = t * channelsPerTask; c < channels.length && c < (t + 1) * channelsPerTask; c++) {
                channelSubset += ((channelSubset == "") ? "" : ",") + channels[c];
            }
            Map<String, String> config = new HashMap<>();
            config.put(IRC_HOST_CONFIG, host);
            config.put(IRC_PORT_CONFIG, port);
            config.put(TOPIC_CONFIG, topic);
            config.put(IRC_CHANNELS_CONFIG, channelSubset);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(IRC_HOST_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "IRC server host name, e.g `irc.wikimedia.org`")
                .define(IRC_PORT_CONFIG, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "IRC server port, e.g. `6667`")
                .define(IRC_CHANNELS_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "coma-separated list of IRC channels to subscribe to, e.g. `#en.wikipedia`")
                .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "kafka topic to publish the feed into");
    }


}

