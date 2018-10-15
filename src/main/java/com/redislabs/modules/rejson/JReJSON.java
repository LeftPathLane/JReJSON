/*
 * BSD 2-Clause License
 *
 * Copyright (c) 2017, Redis Labs
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.redislabs.modules.rejson;

import com.google.gson.Gson;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;

/**
 * JReJSON is the main ReJSON client class, wrapping connection management and all ReJSON commands
 */
public class JReJSON {

    private static Gson gson = new Gson();

    private enum Command implements ProtocolCommand {
        DEL("JSON.DEL"),
        GET("JSON.GET"),
        SET("JSON.SET"),
        TYPE("JSON.TYPE"),
        MGET("JSON.MGET"),
        NUMINCRBY("JSON.NUMINCRBY"),
        NUMMULTBY("JSON.NUMMULTBY"),
        OBJKEYS("JSON.OBJKEYS"),
        OBJLEN("JSON.OBJLEN"),
        STRAPPEND("JSON.STRAPPEND"),
        STRLEN("JSON.STRLEN"),
        ARRAPPEND("JSON.ARRAPPEND"),
        ARRINDEX("JSON.ARRINDEX"),
        ARRINSERT("JSON.ARRINSERT"),
        ARRLEN("JSON.ARRLEN"),
        ARRPOP("JSON.ARRPOP"),
        ARRTRIM("JSON.ARRTRIM");
        private final byte[] raw;

        Command(String alt) {
            raw = SafeEncoder.encode(alt);
        }

        public byte[] getRaw() {
            return raw;
        }
    }

    /**
     * Existential modifier for the set command, by default we don't care
     */
    public enum ExistenceModifier implements ProtocolCommand {
        DEFAULT(""),
        NOT_EXISTS("NX"),
        MUST_EXIST("XX");
        private final byte[] raw;

        ExistenceModifier(String alt) {
            raw = SafeEncoder.encode(alt);
        }

        public byte[] getRaw() {
            return raw;
        }
    }

    /**
     *  Helper to check for errors and throw them as an exception
     * @param str the reply string to "analyze"
     * @throws RuntimeException
     */
    private static void assertReplyNotError(final String str) {
        if (str == null) return;
        if (str.startsWith("-ERR"))
            throw new RuntimeException(str.substring(5));
    }

    /**
     * Helper to check for an OK reply
     * @param str the reply string to "scrutinize"
     */
    private static void assertReplyOK(final String str) {
        if (!str.equals("OK"))
            throw new RuntimeException(str);
    }

    /**
     * Helper to handle single optional path argument situations
     * @param path a single optional path
     * @return the provided path or root if not
     */
    private static Path getSingleOptionalPath(Path... path) {
        // check for 0, 1 or more paths
        if (1 > path.length)
            // default to root
            return Path.RootPath();
         else if (1 == path.length)
            // take 1
            return path[0];
         else
            // throw out the baby with the water
            throw new RuntimeException("Only a single optional path is allowed");
    }

    /**
     * Deletes a path
     * @param conn the Jedis connection
     * @param key the key name
     * @param path optional single path in the object, defaults to root
     * @return the number of paths deleted (0 or 1)
     */
    public static Long del(Jedis conn, String key, Path... path) {

        List<byte[]> args = new ArrayList(2);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));

        conn.getClient().sendCommand(Command.DEL, args.toArray(new byte[args.size()][]));
        return conn.getClient().getIntegerReply();
    }

    /**
     * Gets an object
     * @param conn the Jedis connection
     * @param key the key name
     * @param paths optional one ore more paths in the object, defaults to root
     * @return the requested object
     */
    public static Object get(Jedis conn, String key, Path... paths) {

        List<byte[]> args = new ArrayList(2);

        args.add(SafeEncoder.encode(key));
        for (Path p :paths) {
            args.add(SafeEncoder.encode(p.toString()));
        }
        conn.getClient().sendCommand(Command.GET, args.toArray(new byte[args.size()][]));
        String rep = conn.getClient().getBulkReply();

        assertReplyNotError(rep);
        return gson.fromJson(rep, Object.class);
    }

    /**
     * Sets an object
     * @param conn the Jedis connection
     * @param key the key name
     * @param object the Java object to store
     * @param flag an existential modifier
     * @param path optional single path in the object, defaults to root
     */
    public static void set(Jedis conn, String key, Object object, ExistenceModifier flag, Path... path) {

        List<byte[]> args = new ArrayList(4);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));
        args.add(SafeEncoder.encode(gson.toJson(object)));
        if (ExistenceModifier.DEFAULT != flag) {
            args.add(flag.getRaw());
        }

        conn.getClient().sendCommand(Command.SET, args.toArray(new byte[args.size()][]));
        String status = conn.getClient().getStatusCodeReply();

        assertReplyOK(status);
    }

    /**
     * Sets an object without caring about target path existing
     * @param conn the Jedis connection
     * @param key the key name
     * @param object the Java object to store
     * @param path optional single path in the object, defaults to root
     */
    public static void set(Jedis conn, String key, Object object, Path... path) {
        set(conn,key, object, ExistenceModifier.DEFAULT, path);
    }

    /**
     * Gets the class of an object
     * @param conn the Jedis connection
     * @param key the key name
     * @param path optional single path in the object, defaults to root
     * @return the Java class of the requested object
     */
    public static Class<?> type(Jedis conn, String key, Path... path) {

        List<byte[]> args = new ArrayList(2);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));

        conn.getClient().sendCommand(Command.TYPE, args.toArray(new byte[args.size()][]));
        String rep = conn.getClient().getBulkReply();

        assertReplyNotError(rep);

        switch (rep) {
            case "null":
                return null;
            case "boolean":
                return boolean.class;
            case "integer":
                return int.class;
            case "number":
                return float.class;
            case "string":
                return String.class;
            case "object":
                return Object.class;
            case "array":
                return List.class;
            default:
                throw new java.lang.RuntimeException(rep);
        }
    }

    /**
     * Gets the values at the specified path
     * @param conn the Jedis connection
     * @param path the path to the key
     * @param keys the keys names to retrieve the path from
     * @return a list of values at path from the key(s)
     */
    public static List<String> mget(Jedis conn, Path path, String... keys) {
        List<byte[]> args = new ArrayList(2);
        for (String key : keys) {
            args.add(SafeEncoder.encode(key));
        }
        args.add(SafeEncoder.encode(path.toString()));

        conn.getClient().sendCommand(Command.MGET, args.toArray(new byte[args.size()][]));
        List<String> rep = conn.getClient().getMultiBulkReply();

        if (rep.size()>=1) {
            assertReplyNotError(rep.get(0));
        }
        return rep;
    }

    /**
     * Increments the number value stored at the specified path by specified increment
     * @param conn the Jedis connection
     * @param key the key name
     * @param path the path to the number
     * @param increment the value to increment by
     * @return the Long value of the new value
     */
    public static Long numincrby(Jedis conn, String key, Path path, double increment) {
        List<byte[]> args = new ArrayList(3);
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));
        args.add(SafeEncoder.encode(String.valueOf(increment)));

        conn.getClient().sendCommand(Command.NUMINCRBY, args.toArray(new byte[args.size()][]));
        String rep = conn.getClient().getBulkReply();

        assertReplyNotError(rep);

        return Long.valueOf(rep);
    }

    /**
     * Multiplies the number value stored at path by multiplier
     * @param conn the Jedis connection
     * @param key the key name
     * @param path the path to the number
     * @param multiplier the value to multiply by
     * @return the Long value fo the new value
     */
    public static Long nummultby(Jedis conn, String key, Path path, double multiplier) {
        List<byte[]> args = new ArrayList(3);
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));
        args.add(SafeEncoder.encode(String.valueOf(multiplier)));

        conn.getClient().sendCommand(Command.NUMMULTBY, args.toArray(new byte[args.size()][]));
        String rep = conn.getClient().getBulkReply();

        assertReplyNotError(rep);

        return Long.valueOf(rep);
    }

    /**
     * Returns the list of keys for the object from the specified key with the specified path
     * @param conn the Jedis connection
     * @param key the key name
     * @param path optional single path in the object, defaults to root
     * @return a List of keys that the object holds at the specified path
     */
    public static List<String> objkeys(Jedis conn, String key, Path... path) {
        List<byte[]> args = new ArrayList(2);
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));

        conn.getClient().sendCommand(Command.OBJKEYS, args.toArray(new byte[args.size()][]));
        List<String> rep = conn.getClient().getMultiBulkReply();

        if (rep.size()>=1) {
            assertReplyNotError(rep.get(0));
        }
        return rep;
    }

    /**
     * Returns the number of keys in the object at the path in the key
     * @param conn the Jedis connection
     * @param key the key name
     * @param path optional single path in the object, defaults to root
     * @return a Long count of the amount of keys in the object at the specified path
     */
    public static Long objlen(Jedis conn, String key, Path... path) {
        List<byte[]> args = new ArrayList(2);
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));

        conn.getClient().sendCommand(Command.OBJLEN, args.toArray(new byte[args.size()][]));
        return conn.getClient().getIntegerReply();
    }

    /**
     * Appends the value to the string at the path for the specified key
     * @param conn the Jedis connection
     * @param key the key name
     * @param value the value to append
     * @param path optional single path in the object, defaults to root
     * @return the strings new length
     */
    public static Long strappend(Jedis conn, String key, String value, Path... path) {
        List<byte[]> args = new ArrayList(3);
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));
        args.add(SafeEncoder.encode(gson.toJson(value)));

        conn.getClient().sendCommand(Command.STRAPPEND, args.toArray(new byte[args.size()][]));
        return conn.getClient().getIntegerReply();
    }

    /**
     * Returns the length of the string at the specified path in the specified key
     * @param conn the Jedis connection
     * @param key the key name
     * @param path optional single path in the object, defaults to root
     * @return the Strings length
     */
    public static Long strlen(Jedis conn, String key, Path... path) {
        List<byte[]> args = new ArrayList(2);
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));

        conn.getClient().sendCommand(Command.STRLEN, args.toArray(new byte[args.size()][]));
        return conn.getClient().getIntegerReply();
    }

    /**
     * Appends the values to the array at the specified path of the specified key
     * @param conn the Jedis connection
     * @param key the key name
     * @param path the path to the array of the object
     * @param values the values to add to the array
     * @return the arrays new length
     */
    public static Long arrappend(Jedis conn, String key, Path path, String... values) {
        List<byte[]> args = new ArrayList(3);
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));
        for (String value : values) {
            args.add(SafeEncoder.encode(gson.toJson(value)));
        }

        conn.getClient().sendCommand(Command.ARRAPPEND, args.toArray(new byte[args.size()][]));
        return conn.getClient().getIntegerReply();
    }

    /**
     * Returns the first index of the scalar value in the array at the specified path in the given key
     * @param conn the Jedis connection
     * @param key the key name
     * @param path the path to objects array
     * @param scalar the value to search for
     * @param start the optional inclusive start for the range of array to search
     * @param stop the optional exclusive stop for the range of array to search
     * @return the 0-based index for the position of the scaler or -1 if not found
     */
    public static Long arrindex(Jedis conn, String key, Path path, String scalar, int start, int stop) {
        List<byte[]> args = new ArrayList(3);
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));
        args.add(SafeEncoder.encode(gson.toJson(scalar)));
        if (stop != 0) {
            args.add(SafeEncoder.encode(String.valueOf(start)));
            args.add(SafeEncoder.encode(String.valueOf(stop)));
        } else if (start != 0) {
            args.add(SafeEncoder.encode(String.valueOf(start)));
        }

        conn.getClient().sendCommand(Command.ARRINDEX, args.toArray(new byte[args.size()][]));
        return conn.getClient().getIntegerReply();
    }

    /**
     * Inserts the value(s) into the array at the specified path before the index for the key
     * @param conn the Jedis connection
     * @param key the key name
     * @param path the path to the objects array
     * @param index the index to insert before
     * @param values the values to insert
     * @return the Long value for the new length of the array
     */
    public static Long arrinsert(Jedis conn, String key, Path path, int index, String... values) {
        List<byte[]> args = new ArrayList(4);
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));
        args.add(SafeEncoder.encode(String.valueOf(index)));
        for (String value : values) {
            args.add(SafeEncoder.encode(gson.toJson(value)));
        }

        conn.getClient().sendCommand(Command.ARRINSERT, args.toArray(new byte[args.size()][]));
        return conn.getClient().getIntegerReply();
    }

    /**
     * Returns the length of the array at the path in the key
     * @param conn the Jedis connection
     * @param key the key name
     * @param path optional single path in the object, defaults to root
     * @return the Long value for the length of the array
     */
    public static Long arrlen(Jedis conn, String key, Path... path) {
        List<byte[]> args = new ArrayList(2);
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));

        conn.getClient().sendCommand(Command.ARRLEN, args.toArray(new byte[args.size()][]));
        return conn.getClient().getIntegerReply();
    }

    /**
     * Removes and returns the element from the index in the array at the given path for the key
     * @param conn the Jedis conneciton
     * @param key the key name
     * @param path the path to the array
     * @param index the index to remove from -1 is the final element of the array
     * @return the value that was popped or null if the array was empty
     */
    public static String arrpop(Jedis conn, String key, Path path, int index) {
        List<byte[]> args = new ArrayList(2);
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));
        if (index != -1)
            args.add(SafeEncoder.encode(String.valueOf(index)));

        conn.getClient().sendCommand(Command.ARRPOP, args.toArray(new byte[args.size()][]));
        String rep = conn.getClient().getBulkReply();

        assertReplyNotError(rep);

        return rep;
    }

    /**
     * Trims the array at the specified path for the specified key to the start and stop indexes
     * if start is larger then the length of the array or the start is greater then the stop then the array will be cleared
     * if start < 0 it will default to 0 if stop is larger then the end of the array it will be treated like the last element
     * @param conn the Jedis connection
     * @param key the key name
     * @param path the path to the array
     * @param start the start index to trim from
     * @param stop the end index to stop trimming at
     * @return the Long value of the length of the new array
     */
    public static Long arrtrim(Jedis conn, String key, Path path, int start, int stop) {
        List<byte[]> args = new ArrayList(4);
        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));
        args.add(SafeEncoder.encode(String.valueOf(start)));
        args.add(SafeEncoder.encode(String.valueOf(stop)));

        conn.getClient().sendCommand(Command.ARRTRIM, args.toArray(new byte[args.size()][]));
        return conn.getClient().getIntegerReply();
    }
}
