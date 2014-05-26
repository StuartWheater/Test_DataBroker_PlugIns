/*
 * Copyright (c) 2013-2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.databroker.plugins.test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import org.json.JSONObject;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlowNode;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataService;

public class JSONMongoDBDataService implements DataService, DataProvider<JSONObject>, DataConsumer<JSONObject>
{
    private static final Logger logger = Logger.getLogger(JSONMongoDBDataService.class.getName());

    public JSONMongoDBDataService(String name, Map<String, String> properties)
    {
        logger.info("JSONMongoDBDataService: " + name + ", " + properties);

        _name       = name;
        _properties = properties;
    }

    public String getName()
    {
        return _name;
    }

    public Map<String, String> getProperties()
    {
        return Collections.unmodifiableMap(_properties);
    }

    @Override
    public void consume(DataProvider<JSONObject> dataProvider, JSONObject data)
    {
        logger.info("JSONMongoDBDataService.consume: " + data.toString());
    }

    @Override
    public DataFlowNode getDataFlowNode()
    {
        return this;
    }

    @Override
    public Collection<Class<?>> getDataProviderDataClasses()
    {
        Set<Class<?>> dataProviderDataClasses = new HashSet<Class<?>>();

        dataProviderDataClasses.add(JSONObject.class);
        
        return dataProviderDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataProvider<T> getDataProvider(Class<T> dataClass)
    {
        if (dataClass == JSONObject.class)
            return (DataProvider<T>) this;
        else
            return null;
    }

    @Override
    public Collection<Class<?>> getDataConsumerDataClasses()
    {
        Set<Class<?>> dataConsumerDataClasses = new HashSet<Class<?>>();

        dataConsumerDataClasses.add(JSONObject.class);
        
        return dataConsumerDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataConsumer<T> getDataConsumer(Class<T> dataClass)
    {
        if (dataClass == JSONObject.class)
            return (DataConsumer<T>) this;
        else
            return null;
    }

    @Override
    public Collection<DataConsumer<JSONObject>> getDataConsumers()
    {
        return _dataConsumers;
    }

    @Override
    public void addDataConsumer(DataConsumer<JSONObject> dataConsumer)
    {
        _dataConsumers.add(dataConsumer);
    }

    @Override
    public void removeDataConsumer(DataConsumer<JSONObject> dataConsumer)
    {
        _dataConsumers.remove(dataConsumer);
    }

    @Override
    public void produce(JSONObject data)
    {
        for (DataConsumer<JSONObject> dataConsumer: _dataConsumers)
            dataConsumer.consume(this, data);
    }

    private String                         _name;
    private Map<String, String>            _properties;
    private List<DataConsumer<JSONObject>> _dataConsumers;
}
