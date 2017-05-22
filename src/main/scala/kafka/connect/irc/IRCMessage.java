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
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.schwering.irc.lib.IRCUser;

/**
 * Kafka Connect Domain object representing a generic IRC Message with its SCHEMA descriptor.
 */

public class IRCMessage extends Struct {

    final public static Schema SCHEMA = SchemaBuilder.struct().name("IRCMessage").version(1)
            .field("timestamp", Schema.INT64_SCHEMA)
            .field("channel", Schema.STRING_SCHEMA)
            .field("message", Schema.STRING_SCHEMA)
            .field("user", SchemaBuilder.struct().name("IRCUser").version(1)
                    .field("nick", Schema.STRING_SCHEMA)
                    .field("username", Schema.STRING_SCHEMA)
                    .field("host", Schema.STRING_SCHEMA)
                    .build()
            )
            .build();

    public IRCMessage(String channel, IRCUser user, String message) {
        super(SCHEMA);
        this.put("timestamp", System.currentTimeMillis());
        this.put("channel", channel);
        this.put("message", message);
        this.put("user", new Struct(SCHEMA.field("user").schema())
                .put("nick", user.getNick())
                .put("username", user.getUsername())
                .put("host", user.getHost())
        );
    }

    @Override
    public String toString() {
        return "IRCMessage " + get("timestamp") + "\t" + get("channel") + "\t" + get("message");
    }

}