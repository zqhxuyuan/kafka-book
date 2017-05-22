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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.schwering.irc.lib.IRCConnection;
import org.schwering.irc.lib.IRCEventAdapter;
import org.schwering.irc.lib.IRCUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class IRCFeedTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(IRCFeedTask.class);

    private static final Schema KEY_SCHEMA = Schema.INT64_SCHEMA;
    private static final String TIMESTAMP_FIELD = "timestamp";
    private static final String CHANNEL_FIELD = "channel";

    private BlockingQueue<SourceRecord> queue = null;
    private String[] channels = null;
    private String topic = null;
    private String host;
    private int port;
    private IRCConnection conn;

    @Override
    public String version() {
        return new IRCFeedConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        queue = new LinkedBlockingQueue<>();
        host = props.get(IRCFeedConnector.IRC_HOST_CONFIG);
        port = Integer.parseInt(props.get(IRCFeedConnector.IRC_PORT_CONFIG));
        channels = props.get(IRCFeedConnector.IRC_CHANNELS_CONFIG).split(",");
        topic = props.get(IRCFeedConnector.TOPIC_CONFIG);


        String nick = "kafka-connect-irc-" + Math.abs(new Random().nextInt());
        log.info("Starting irc feed task " + nick + ", channels " + props.get(IRCFeedConnector.IRC_CHANNELS_CONFIG));
        this.conn = new IRCConnection(host, new int[]{port}, "", nick, nick, nick);
        this.conn.addIRCEventListener(new IRCMessageListener());
        this.conn.setEncoding("UTF-8");
        this.conn.setPong(true);
        this.conn.setColors(false);
        try {
            this.conn.connect();
        } catch (IOException e) {
            throw new RuntimeException("Unable to connect to " + host + ":" + port + ".", e);
        }
        for (String channel : channels) {
            this.conn.send("JOIN " + channel);
        }

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> result = new LinkedList<>();
        if (queue.isEmpty()) result.add(queue.take());
        queue.drainTo(result);
        return result;
    }

    @Override
    public void stop() {
        for (String channel : channels) {
            try {
                conn.send("PART " + channel);
            } catch (Throwable e) {
                log.warn("Problem leaving channel ", e);
            }
        }
        this.conn.interrupt();
        try {
            this.conn.join();
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while trying to shutdown IRC connection for " + host + ":" + port, e);
        }

        if (this.conn.isAlive()) {
            throw new RuntimeException("Unable to shutdown IRC connection for " + host + ":" + port);
        }
        queue.clear();
    }

    class IRCMessageListener extends IRCEventAdapter {
        @Override
        public void onPrivmsg(String channel, IRCUser u, String msg) {
            IRCMessage event = new IRCMessage(channel, u, msg);
            //FIXME kafka round robin default partitioner seems to always publish to partition 0 only (?)
            long ts = event.getInt64("timestamp");
            Map<String, ?> srcOffset = Collections.singletonMap(TIMESTAMP_FIELD, ts);
            Map<String, ?> srcPartition = Collections.singletonMap(CHANNEL_FIELD, channel);
            SourceRecord record = new SourceRecord(srcPartition, srcOffset, topic, KEY_SCHEMA, ts, IRCMessage.SCHEMA, event);
            queue.offer(record);
        }

        @Override
        public void onError(String msg) {
            log.warn("IRC Error: " + msg);
        }
    }

}
