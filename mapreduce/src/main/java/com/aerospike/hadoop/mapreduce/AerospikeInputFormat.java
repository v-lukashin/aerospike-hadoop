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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.async.AsyncClientPolicy;
import com.aerospike.client.cluster.Node;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * An {@link InputFormat} for data stored in an Aerospike database.
 */
public class AerospikeInputFormat
        extends InputFormat<AerospikeKey, AerospikeRecord>
        implements org.apache.hadoop.mapred.InputFormat<AerospikeKey,
        AerospikeRecord> {

    private static final Log log =
            LogFactory.getLog(AerospikeInputFormat.class);

    // ---------------- NEW API ----------------

    public List<InputSplit> getSplits(JobContext context) throws IOException {
        // Delegate to the old API.
        Configuration cfg = context.getConfiguration();
        JobConf jobconf = AerospikeConfigUtil.asJobConf(cfg);
        return Arrays.asList((InputSplit[]) getSplits(jobconf,
                jobconf.getNumMapTasks()));
    }

    public RecordReader<AerospikeKey, AerospikeRecord>
    createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new AerospikeRecordReader();
    }

    // ---------------- OLD API ----------------

    public org.apache.hadoop.mapred.InputSplit[]
    getSplits(JobConf job, int numSplits) throws IOException {
        try {
            Host[] hosts;
            String host = AerospikeConfigUtil.getInputHost(job);

            String[] splitHosts = host.split(",");

            if (splitHosts.length == 0) {
                log.error("Output host is empty");
                return null;
            } else {
                hosts = new Host[splitHosts.length];
                for (int i = 0; i < hosts.length; i++) {
                    String[] hostPort = splitHosts[i].split(":");
                    int port = hostPort.length < 2 ? AerospikeConfigUtil.getOutputPort(job) : Integer.parseInt(hostPort[1]);
                    hosts[i] = new Host(hostPort[0], port);
                }
            }

            String oper = AerospikeConfigUtil.getInputOperation(job);
            String namespace = AerospikeConfigUtil.getInputNamespace(job);
            String setName = AerospikeConfigUtil.getInputSetName(job);
            String[] binNames = AerospikeConfigUtil.getInputBinNames(job);
            String numrangeBin = "";
            long numrangeBegin = 0;
            long numrangeEnd = 0;
            if (oper.equals("numrange")) {
                numrangeBin = AerospikeConfigUtil.getInputNumRangeBin(job);
                numrangeBegin = AerospikeConfigUtil.getInputNumRangeBegin(job);
                numrangeEnd = AerospikeConfigUtil.getInputNumRangeEnd(job);
            }

            log.info(String.format("using: %s %s %s",
                    Arrays.toString(hosts), namespace, setName));
            AsyncClientPolicy policy = new AsyncClientPolicy();

            policy.user = "";
            policy.password = "";
            policy.failIfNotConnected = true;
            AerospikeClient client =
                    AsyncClientSingleton.getInstance(policy, hosts);
            Node[] nodes = client.getNodes();
            int nsplits = nodes.length;
            if (nsplits == 0) {
                throw new IOException("no Aerospike nodes found");
            }
            log.info(String.format("found %d nodes", nsplits));
            AerospikeSplit[] splits = new AerospikeSplit[nsplits];
            for (int ii = 0; ii < nsplits; ii++) {
                Node node = nodes[ii];
                String nodeName = node.getName();

                // We want to avoid 127.0.0.1 as a hostname
                // because this value will be transferred to a
                // different hadoop node to be processed.
                //
                Host[] aliases = node.getAliases();
                Host nodehost = aliases[0];
                if (aliases.length > 1) {
                    for (int jj = 0; jj < aliases.length; ++jj) {
                        if (!aliases[jj].name.equals("127.0.0.1")) {
                            nodehost = aliases[jj];
                            break;
                        }
                    }
                }
                splits[ii] = new AerospikeSplit(oper, nodeName,
                        nodehost.name, nodehost.port,
                        namespace, setName, binNames,
                        numrangeBin, numrangeBegin,
                        numrangeEnd);
                log.info("split: " + splits[ii]);
            }
            return splits;
        } catch (Exception ex) {
            throw new IOException("exception in getSplits", ex);
        }
    }

    public org.apache.hadoop.mapred.RecordReader<AerospikeKey, AerospikeRecord>
    getRecordReader(org.apache.hadoop.mapred.InputSplit split,
                    JobConf job,
                    Reporter reporter
    ) throws IOException {
        return new AerospikeRecordReader((AerospikeSplit) split);
    }

}

// Local Variables:
// mode: java
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: nil
// End:
// vim: softtabstop=4:shiftwidth=4:expandtab
