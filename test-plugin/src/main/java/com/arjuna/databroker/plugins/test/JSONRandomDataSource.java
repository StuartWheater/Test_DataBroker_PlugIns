/*
 * Copyright (c) 2013-2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.databroker.plugins.test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONObject;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlow;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataSource;
import com.arjuna.databroker.data.jee.annotation.DataConsumerInjection;
import com.arjuna.databroker.data.jee.annotation.DataProviderInjection;

public class JSONRandomDataSource implements DataSource
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

                    _dataProvider.produce(jsonObject);

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

        _name       = name;
        _properties = properties;
        _dataFlow   = null;

        (new Worker()).start();
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

    private String                   _name;
    private Map<String, String>      _properties;
    private DataFlow                 _dataFlow;
    @DataProviderInjection
    private DataProvider<JSONObject> _dataProvider;
}
