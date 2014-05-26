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
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.LinkedList;
import org.json.JSONObject;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlowNode;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataSource;

public class JSONRandomDataSource implements DataSource, DataProvider<JSONObject>
{
    private static final Logger logger = Logger.getLogger(JSONRandomDataSource.class.getName());

    private class Worker extends Thread
    {
        public void run()
        {
            try
            {
                while (true)
                {
                    JSONObject jsonObject = new JSONObject();

                    jsonObject.put("Key1", UUID.randomUUID().toString());
                    jsonObject.put("Key2", UUID.randomUUID().toString());
                    jsonObject.put("Key3", UUID.randomUUID().toString());
                    jsonObject.put("Key4", UUID.randomUUID().toString());

                    produce(jsonObject);

                    Thread.sleep(Long.parseLong(_properties.get(TIMEINTERVAL_PROPERTYNAME)));
                }
            }
            catch (Throwable throwable)
            {
                logger.log(Level.WARNING, "Problem in JSONRandomDataSource.Worker", throwable);
            }
        }
    }

    public static final String TIMEINTERVAL_PROPERTYNAME = "Time Interval";

    public JSONRandomDataSource(String name, Map<String, String> properties)
    {
        logger.info("JSONRandomDataSource: " + name + ", " + properties);

        _name          = name;
        _properties    = properties;
        _dataConsumers = new LinkedList<DataConsumer<JSONObject>>();

        (new Worker()).start();
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
