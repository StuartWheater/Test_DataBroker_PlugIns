/*
 * Copyright (c) 2013-2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.databroker.plugins.twitter;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.LinkedList;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlowNode;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataSource;

public class TwitterDataSource implements DataSource, DataProvider<String>
{
    private static final Logger logger = Logger.getLogger(TwitterDataSource.class.getName());

    public static final String TIMEINTERVAL_PROPERTYNAME = "Time Interval";

    public TwitterDataSource(String name, Map<String, String> properties)
    {
        logger.info("TwitterDataSource: " + name + ", " + properties);

        _name          = name;
        _properties    = properties;
        _dataConsumers = new LinkedList<DataConsumer<String>>();
        
        // Worker Thread goes here
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
    public DataFlowNode getDataFlowNode()
    {
        return this;
    }

    @Override
    public Collection<Class<?>> getDataProviderDataClasses()
    {
        Set<Class<?>> dataProviderDataClasses = new HashSet<Class<?>>();

        dataProviderDataClasses.add(String.class);
        
        return dataProviderDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataProvider<T> getDataProvider(Class<T> dataClass)
    {
        if (dataClass == String.class)
            return (DataProvider<T>) this;
        else
            return null;
    }

    @Override
    public Collection<DataConsumer<String>> getDataConsumers()
    {
        return _dataConsumers;
    }

    @Override
    public void addDataConsumer(DataConsumer<String> dataConsumer)
    {
        _dataConsumers.add(dataConsumer);
    }

    @Override
    public void removeDataConsumer(DataConsumer<String> dataConsumer)
    {
        _dataConsumers.remove(dataConsumer);
    }

    @Override
    public void produce(String data)
    {
        for (DataConsumer<String> dataConsumer: _dataConsumers)
            dataConsumer.consume(this, data);
    }

    private String                     _name;
    private Map<String, String>        _properties;
    private List<DataConsumer<String>> _dataConsumers;
}
