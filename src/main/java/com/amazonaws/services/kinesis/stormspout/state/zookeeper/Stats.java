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

import java.util.HashMap;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Stats {
    private static final Logger LOG = LoggerFactory.getLogger(Stats.class);
    private final HashMap<String, Object> stats;
    private final ArrayList<String> errors;

    /**
     * Constructor.
     * 
     * @param records Kinesis records
     * @param endOfShard Did we reach the end of the shard?
     */
    Stats() {
        this.stats = new HashMap<String, Object>();
        this.errors = new ArrayList<String>();
    }
   
    void addError(String error) {
        this.errors.add(error);
    }
    
    void replaceStats(String key, Object value) {
        stats.put(key,value);
    }
    
    void incStats(String key) {
        if (this.stats.containsKey(key)) {
            Long currentVal = (Long) this.stats.get(key);
            this.stats.put(key, currentVal+1);
        } else {
            this.stats.put(key, new Long(1));
        }        
    }
    
    void decStats (String key) {
        if (this.stats.containsKey(key)) {
            Long currentVal = (Long) this.stats.get(key);
            this.stats.put(key, currentVal-1);
        } else {
            this.stats.put(key, new Long(-1));
        }          
    }
    
    
    void printStats () {
        LOG.info("---------> KINESIS SPOUT STATS <------------");
        for (String name: stats.keySet()) {
            String key = name.toString();
            String value = stats.get(name).toString();
            LOG.info(key + ":" + value);
        }
        
        for (String error : errors) {
            LOG.info(error);
        }
        LOG.info("<--------- KINESIS SPOUT STATS ------------>");
    }

}
