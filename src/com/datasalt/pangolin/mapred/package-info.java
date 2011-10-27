/**
 * This package contains the Map/Reduce Jobs that are part of the PISAE pipeline as defined here:
 * https://docs.google.com/a/datasalt.com/document/d/1Hownedp5RwYRVdVcci8p4tQPgjjG89cUEKAGqyClLZk/edit?hl=en_US
 * <p>
 * There are 2 basic classes: {@link PangolinMapper}, {@link PangolinReducer} which extend {@link InjectedMapper} 
 * and {@link InjectedReducer}. These base classes can be used to implement Mappers or Reducers that can receive injected 
 * instances or configuration through Guice.
 * <p>
 * The sub-package "joiner" contains a generic Multi-type Joiner that can be used to implement Map/Reduce Jobs that read
 * input data from several data sources and need to join them in the Reducer according to some grouping.
 **/
package com.datasalt.pangolin.mapred;
import com.datasalt.pangolin.mapred.InjectedMapper;
import com.datasalt.pangolin.mapred.InjectedReducer;

