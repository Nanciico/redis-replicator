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

import com.moilioncircle.redis.replicator.cmd.CommandParser;
import com.moilioncircle.redis.replicator.cmd.impl.FlushAllCommand;

import static com.moilioncircle.redis.replicator.cmd.CommandParsers.toRune;
import static com.moilioncircle.redis.replicator.util.Strings.isEquals;

/**
 * @author Leon Chen
 * @since 2.1.0
 */
public class FlushAllParser implements CommandParser<FlushAllCommand> {
    @Override
    public FlushAllCommand parse(Object[] command) {
        boolean isAsync = false;
        boolean isSync = false;
        if (command.length == 2 && isEquals(toRune(command[1]), "ASYNC")) {
            isAsync = true;
        } else if (command.length == 2 && isEquals(toRune(command[1]), "SYNC")) {
            isSync = true;
        }
        return new FlushAllCommand(isAsync, isSync);
    }

}
