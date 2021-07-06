/*
 * Copyright 2016-2018 Leon Chen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moilioncircle.redis.replicator.cmd.parser;

import static com.moilioncircle.redis.replicator.cmd.CommandParsers.toBytes;
import static com.moilioncircle.redis.replicator.cmd.CommandParsers.toInt;

import com.moilioncircle.redis.replicator.cmd.CommandParser;
import com.moilioncircle.redis.replicator.cmd.impl.ZDiffStoreCommand;

/**
 * @author Leon Chen
 * @since 3.5.0
 */
public class ZDiffStoreParser implements CommandParser<ZDiffStoreCommand> {
    
    @Override
    public ZDiffStoreCommand parse(Object[] command) {
        int idx = 1;
        byte[] destination = toBytes(command[idx]);
        idx++;
        int numkeys = toInt(command[idx++]);
        byte[][] keys = new byte[numkeys][];
        for (int i = 0; i < numkeys; i++) {
            keys[i] = toBytes(command[idx]);
            idx++;
        }
        return new ZDiffStoreCommand(destination, numkeys, keys);
    }
}