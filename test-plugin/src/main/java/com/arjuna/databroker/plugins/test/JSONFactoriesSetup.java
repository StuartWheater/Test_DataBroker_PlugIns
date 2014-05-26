/*
 * Copyright (c) 2013-2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.databroker.plugins.test;

import java.util.Collections;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.Startup;

import com.arjuna.databroker.data.DataFlowNodeFactory;
import com.arjuna.databroker.data.DataFlowNodeFactoryInventory;

@Startup
@Singleton
public class JSONFactoriesSetup
{
    @PostConstruct
    public void setup()
    {
        DataFlowNodeFactory jsonRandomDataSourceFactory    = new JSONRandomDataSourceFactory("Test JSON Random Data Source Factory", Collections.<String, String>emptyMap());
        DataFlowNodeFactory jsonFilterDataProcessorFactory = new JSONFilterDataProcessorFactory("Test JSON Filter Data Processor Factory", Collections.<String, String>emptyMap());
        DataFlowNodeFactory jsonLoggingDataSinkFactory     = new JSONLoggingDataSinkFactory("Test JSON Logging Data Sink Factory", Collections.<String, String>emptyMap());
        DataFlowNodeFactory jsonMongoDBDataStoreFactory    = new JSONMongoDBDataStoreFactory("Test JSON MongoDB Data Store Factory", Collections.<String, String>emptyMap());
        DataFlowNodeFactory jsonMongoDBDataServiceFactory  = new JSONMongoDBDataServiceFactory("Test JSON MongoDB Data Service Factory", Collections.<String, String>emptyMap());

        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(jsonRandomDataSourceFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(jsonFilterDataProcessorFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(jsonLoggingDataSinkFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(jsonMongoDBDataStoreFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(jsonMongoDBDataServiceFactory);
    }

    @PreDestroy
    public void cleanup()
    {
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Test JSON Random Data Source Factory");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Test JSON Filter Data Processor Factory");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Test JSON Logging Data Sink Factory");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Test JSON MongoDB Data Store Factory");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Test JSON MongoDB Data Service Factory");
    }

    @EJB(lookup="java:global/server-ear-1.0.0p1m1/control-core-1.0.0p1m1/DataFlowNodeFactoryInventory")
    private DataFlowNodeFactoryInventory _dataFlowNodeFactoryInventory;
}