package com.aerospike.hadoop.mapreduce;

import com.aerospike.client.Host;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.AsyncClientPolicy;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class AsyncClientPool {

    private static final ConcurrentHashMap<String, AsyncClient> instances = new ConcurrentHashMap<String, AsyncClient>();

    public static AsyncClient getInstance(AsyncClientPolicy policy,
                                          Host... hosts) {
        String[] strHosts = new String[hosts.length];
        for (int i = 0; i < hosts.length; i++) strHosts[i] = hosts[i].toString();
        Arrays.sort(strHosts);

        StringBuilder sb = new StringBuilder();
        for (String strHost : strHosts) sb.append(strHost).append(',');

        String key = sb.toString();
        AsyncClient client;

        if (instances.contains(key)) client = instances.get(key);
        else synchronized (instances) {
            if (instances.contains(key)) client = instances.get(key);
            else instances.put(key, client = new AsyncClient(policy, hosts));
        }
        return client;
    }
}
