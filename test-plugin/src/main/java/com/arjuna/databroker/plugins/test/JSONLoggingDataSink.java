/*
 * Copyright (c) 2013-2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.databroker.plugins.test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import org.json.JSONObject;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlowNode;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataSink;

public class JSONLoggingDataSink implements DataSink, DataConsumer<JSONObject>
{
    private static final Logger logger = Logger.getLogger(JSONLoggingDataSink.class.getName());

    public JSONLoggingDataSink(String name, Map<String, String> properties)
    {
        logger.info("JSONLoggingDataSink: " + name + ", " + properties);

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
        logger.info("JSONLoggingDataSink.consume: " + data.toString());
    }

    @Override
    public DataFlowNode getDataFlowNode()
    {
        return this;
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

    private String              _name;
    private Map<String, String> _properties;
}
