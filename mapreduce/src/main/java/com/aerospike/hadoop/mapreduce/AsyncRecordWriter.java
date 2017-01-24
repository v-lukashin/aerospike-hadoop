/* 
 * Copyright 2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more
 * contributor license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.aerospike.hadoop.mapreduce;

import com.aerospike.client.Host;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.AsyncClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

public abstract class AsyncRecordWriter<KK, VV>
        extends RecordWriter<KK, VV>
        implements org.apache.hadoop.mapred.RecordWriter<KK, VV> {

    private static final Log log =
            LogFactory.getLog(AsyncRecordWriter.class);

    private static final int NO_TASK_ID = -1;

    protected final Configuration cfg;
    protected boolean initialized = false;

    private static String namespace;
    private static String setName;
    private static AsyncClient client;
    private static WritePolicy writePolicy;

    private Progressable progressable;

    public AsyncRecordWriter(Configuration cfg, Progressable progressable) {
        this.cfg = cfg;
        this.progressable = progressable;
    }

    public abstract void writeAerospike(KK key,
                                        VV value,
                                        AsyncClient client,
                                        WritePolicy writePolicy,
                                        String namespace,
                                        String setName) throws IOException;

    @Override
    public void write(KK key, VV value) throws IOException {
        if (!initialized) {
            initialized = true;
            init();
        }

        writeAerospike(key, value, client, writePolicy, namespace, setName);
    }

    protected void init() throws IOException {
        Host[] hosts;
        String host = AerospikeConfigUtil.getOutputHost(cfg);

        String[] splitHosts = host.split(",");

        if (splitHosts.length == 0) {
            log.error("Output host is empty");
            return;
        } else {
            hosts = new Host[splitHosts.length];
            for (int i = 0; i < hosts.length; i++) {
                String[] hostPort = splitHosts[i].split(":");
                int port = hostPort.length < 2 ? AerospikeConfigUtil.getOutputPort(cfg) : Integer.parseInt(hostPort[1]);
                hosts[i] = new Host(hostPort[0], port);
            }
        }

        namespace = AerospikeConfigUtil.getOutputNamespace(cfg);
        setName = AerospikeConfigUtil.getOutputSetName(cfg);

        log.info(String.format("init: %s %s %s",
                host, namespace, setName));

        AsyncClientPolicy policy = new AsyncClientPolicy();

        policy.user = "";
        policy.password = "";
        policy.asyncMaxCommands = AerospikeConfigUtil.getAsyncMaxCommands(cfg);
        policy.failIfNotConnected = true;

        client = AsyncClientSingleton.getInstance(policy, hosts);
        writePolicy = new WritePolicy();
        writePolicy.sendKey = AerospikeConfigUtil.getSendKey(cfg);
        writePolicy.expiration = AerospikeConfigUtil.getExpirationTime(cfg);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
        doClose(context);
    }

    @Override
    public void close(org.apache.hadoop.mapred.Reporter reporter
    ) throws IOException {
        doClose(reporter);
    }

    protected void doClose(Progressable progressable) {
        log.info("doClose");
        initialized = false;
    }
}