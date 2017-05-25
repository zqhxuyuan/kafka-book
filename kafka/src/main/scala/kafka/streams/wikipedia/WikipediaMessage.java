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

package kafka.streams.wikipedia;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.KeyValue;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WikipediaMessage {

    public enum Type {
        EDIT, SPECIAL, TALK
    }

    public Type type;

    public String title;
    public String user;
    public String diffUrl;
    public int byteDiff;
    public String summary;

    public boolean isMinor;
    public boolean isNew;
    public boolean isUnpatrolled;
    public boolean isBotEdit;

    @Override
    public String toString() {
        String flagString = "";
        if (isNew) flagString += "NEW ";
        if (isMinor) flagString += "MINOR ";
        if (isBotEdit) flagString += "BOT ";
        if (isUnpatrolled) flagString += "unpatrolled ";
        return String.format("[%s%s by %s] SUMMARY:%s TITLE: %s, DIFF[%d]: %s "
                , flagString, type, user, summary, title, byteDiff, diffUrl);
    }

    public static KeyValue<String, WikipediaMessage> parceIRC(JsonNode time, JsonNode jsonIRCMessage) {
        try {
            WikipediaMessage edit = WikipediaMessage.parseText(jsonIRCMessage.get("message").textValue());
            return new KeyValue<>(edit.user, edit);
        } catch (IllegalArgumentException e) {
            return new KeyValue<>(null, null);
        }
    }

    public static boolean filterNonNull(String key, WikipediaMessage value) {
        return key != null && value != null;
    }

    private static WikipediaMessage parseText(String raw) {
        Pattern p = Pattern.compile("\\[\\[(.*)\\]\\]\\s(.*)\\s(.*)\\s\\*\\s(.*)\\s\\*\\s\\(\\+?(.\\d*)\\)\\s(.*)");
        Matcher m = p.matcher(raw);

        if (!m.find()) {
            throw new IllegalArgumentException("Could not parse message: " + raw);
        } else if (m.groupCount() != 6) {
            throw new IllegalArgumentException("Unexpected parser group count: " + m.groupCount());
        } else {
            WikipediaMessage result = new WikipediaMessage();

            result.title = m.group(1);
            String flags = m.group(2);
            result.diffUrl = m.group(3);
            result.user = m.group(4);
            result.byteDiff = Integer.parseInt(m.group(5));
            result.summary = m.group(6);

            result.isNew = flags.contains("N");
            result.isMinor = flags.contains("M");
            result.isUnpatrolled = flags.contains("!");
            result.isBotEdit = flags.contains("B");

            result.type = result.title.startsWith("Special:") ? Type.SPECIAL :
                    (result.title.startsWith("Talk:") ? Type.TALK : Type.EDIT);
            return result;
        }
    }

}
