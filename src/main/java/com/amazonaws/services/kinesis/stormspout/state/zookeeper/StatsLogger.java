/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.stormspout.state.zookeeper;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.nio.charset.StandardCharsets;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.stormspout.KinesisSpoutConfig;
import com.amazonaws.services.kinesis.stormspout.exceptions.KinesisSpoutException;
import com.amazonaws.services.kinesis.stormspout.state.zookeeper.NodeFunction.Mod;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.netflix.curator.RetryLoop;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;


class UpdateLog {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateLog.class);
    long delay = 120 * 1000; // delay in milliseconds
    LoopTask task = null;
    Timer timer = null;
    
    public UpdateLog(StatsLogger stl) {
        this.task = new LoopTask(stl);
        this.timer = new Timer("TaskName");
    }

    public void start() {
        timer.cancel();
        timer = new Timer("TaskName");
        Date executionDate = new Date(); // no params = now
        timer.scheduleAtFixedRate(task, executionDate, delay);
    }

    private class LoopTask extends TimerTask {
        StatsLogger stl;
        
        LoopTask (StatsLogger stl) {
            this.stl = stl;
        }
        
        public void run() {
            stl.updateLoggerState();
            if ("true".equals(stl.getLoggerState())) {
                stl.printStats();
            }
        }
    }
} 


public class StatsLogger {
    private static StatsLogger instance = null;
    private static final Logger LOG = LoggerFactory.getLogger(StatsLogger.class);
    private static final int BASE_SLEEP_TIME_MS = 200;
    private static final int BASE_OPTIMISTIC_RETRY_TIME_MS = 100;
    private static final int MAX_NUM_RETRIES = 5;    
    private final String DEBUGSUFFIX = "debug";
    private final KinesisSpoutConfig config;
    private final CuratorFramework zk;
    private String loggerState = "";
    private UpdateLog updateTask = null;
    private Stats stats;

    
    public static StatsLogger getInstance(final KinesisSpoutConfig config) {
        if (StatsLogger.instance == null) {
            StatsLogger.instance = new StatsLogger(config);
        }
        return StatsLogger.instance;
    }
    
    /**
     * Create and configure the ZK sync object with the KinesisSpoutConfig.
     * @param config  the configuration for the spout.
     */
    private StatsLogger(final KinesisSpoutConfig config) {
        this.config = config;
        this.stats = new Stats();

        try {
            zk = CuratorFrameworkFactory.newClient(config.getZookeeperConnectionString(),
                    new ExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_NUM_RETRIES));
            zk.start();
            updateLoggerState();
            this.updateTask = new UpdateLog(this);
            this.updateTask.start();
            }
        catch (IOException e) {
            LOG.error("Could not connect to ZooKeeper", e);
            throw new KinesisSpoutException(e);
        }
    }
    
    
    void updateLoggerState() {
        try {
            byte [] deb = get(DEBUGSUFFIX);
            this.loggerState = new String(deb, StandardCharsets.UTF_8); 
            LOG.info("New Logger state is:"+this.loggerState);
        } catch (Exception e) {
            this.loggerState = "";
        }
    }
    
    String getLoggerState() {
        return this.loggerState;
    }

    
    public void replaceStats(String key, Object value) {
        stats.replaceStats(key,value);
    }
    
    public void incStats(String key) {
        stats.incStats(key);    
    }
    
    public void decStats (String key) {
        stats.decStats(key);         
    }    
    
    public void addError(String error) {
        stats.addError(error);
    }
    
    public void printStats() {
        stats.printStats();
    }


    /**
     * Closes the connection to ZK.
     * 
     * @throws InterruptedException
     */
    void close() throws InterruptedException {
        zk.close();
    }

    private byte[] get(final String pathSuffix) throws Exception {
        return RetryLoop.callWithRetry(zk.getZookeeperClient(), new Callable<byte[]>() {
            @Override
            public byte[] call() throws Exception {
                return zk.getData().forPath(buildZookeeperPath(pathSuffix));
            }
        });
    }

    private String buildZookeeperPath(final String suffix) {
        return "/" + config.getZookeeperPrefix() + "/" + config.getTopologyName() + "/"
               +  config.getStreamName() + "/" + suffix;
    }

}
