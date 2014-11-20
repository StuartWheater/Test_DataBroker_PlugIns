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
import com.arjuna.databroker.data.DataFlow;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataProcessor;
import com.arjuna.databroker.data.jee.annotation.DataConsumerInjection;
import com.arjuna.databroker.data.jee.annotation.DataProviderInjection;

public class JSONMongoDBDataStore implements DataProcessor
{
    private static final Logger logger = Logger.getLogger(JSONMongoDBDataStore.class.getName());

    public static final String REMOVEKEY_PROPERTYNAME = "Remove Key";

    public JSONMongoDBDataStore(String name, Map<String, String> properties)
    {
        logger.info("JSONFilterDataProcessor: " + name + ", " + properties);

        _name       = name;
        _properties = properties;
        _dataFlow   = null;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public void setName(String name)
    {
        _name = name;
    }

    @Override
    public Map<String, String> getProperties()
    {
        return Collections.unmodifiableMap(_properties);
    }

    @Override
    public void setProperties(Map<String, String> properties)
    {
        _properties = properties;
    }

    @Override
    public DataFlow getDataFlow()
    {
        return _dataFlow;
    }

    @Override
    public void setDataFlow(DataFlow dataFlow)
    {
        _dataFlow = dataFlow;
    }

    public void consume(JSONObject data)
    {
        logger.info("JSONFilterDataProcessor.consume: " + data.toString());

        JSONObject results = new JSONObject(data);

        results.remove(REMOVEKEY_PROPERTYNAME);

        _dataProvider.produce(results);
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
            return (DataProvider<T>) _dataProvider;
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
            return (DataConsumer<T>) _dataConsumer;
        else
            return null;
    }

    private String                   _name;
    private Map<String, String>      _properties;
    private DataFlow                 _dataFlow;
    @DataConsumerInjection(methodName="consume")
    private DataConsumer<JSONObject> _dataConsumer;
    @DataProviderInjection
    private DataProvider<JSONObject> _dataProvider;
}
