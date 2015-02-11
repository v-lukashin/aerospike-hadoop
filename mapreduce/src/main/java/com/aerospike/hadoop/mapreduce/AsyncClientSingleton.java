package com.aerospike.hadoop.mapreduce;

import com.aerospike.client.Host;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.AsyncClientPolicy;

public class AsyncClientSingleton {

    private static volatile AsyncClient instance = null;

    public static AsyncClient getInstance(AsyncClientPolicy policy,
                                          Host... hosts) {
        if (instance == null) {
            synchronized (AsyncClientSingleton.class) {
                if (instance == null) {
                    instance = new AsyncClient(policy, hosts);
                }
            }
        }
        return instance;
    }
}
