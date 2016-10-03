package org.infinispan.persistence.cassandra.configuration;

import com.datastax.driver.core.ConsistencyLevel;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.persistence.cassandra.CassandraStore;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:jmarkos@redhat.com">Jakub Markos</a>
 */
@ConfigurationFor(CassandraStore.class)
@BuiltBy(CassandraStoreConfigurationBuilder.class)
public class CassandraStoreConfiguration extends AbstractStoreConfiguration {

   final static AttributeDefinition<Boolean> AUTO_CREATE_KEYSPACE = AttributeDefinition.builder("autoCreateKeyspace", true).immutable().build();
   final static AttributeDefinition<String> KEYSPACE = AttributeDefinition.builder("keyspace", "Infinispan").immutable().build();
   final static AttributeDefinition<String> ENTRY_TABLE = AttributeDefinition.builder("entryTable", "InfinispanEntries").immutable().build();
   final static AttributeDefinition<ConsistencyLevel> READ_CONSISTENCY_LEVEL = AttributeDefinition.builder("readConsistencyLevel", ConsistencyLevel.LOCAL_ONE).immutable().build();
   final static AttributeDefinition<ConsistencyLevel> READ_SERIAL_CONSISTENCY_LEVEL = AttributeDefinition.builder("readSerialConsistencyLevel", ConsistencyLevel.SERIAL).immutable().build();
   final static AttributeDefinition<ConsistencyLevel> WRITE_CONSISTENCY_LEVEL = AttributeDefinition.builder("writeConsistencyLevel", ConsistencyLevel.LOCAL_ONE).immutable().build();
   final static AttributeDefinition<ConsistencyLevel> WRITE_SERIAL_CONSISTENCY_LEVEL = AttributeDefinition.builder("writeSerialConsistencyLevel", ConsistencyLevel.SERIAL).immutable().build();
   final static AttributeDefinition<String> REPLICATION_STRATEGY = AttributeDefinition.builder("replicationStrategy", "{'class':'SimpleStrategy', 'replication_factor':1}").immutable().build();
   final static AttributeDefinition<String> COMPRESSION = AttributeDefinition.builder("compression", "{ }").immutable().build();
   static final AttributeDefinition<List<CassandraStoreServerConfiguration>> SERVERS = AttributeDefinition.builder("servers", null, (Class<List<CassandraStoreServerConfiguration>>) (Class<?>) List.class).initializer(ArrayList::new).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(CassandraStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(),
                              AUTO_CREATE_KEYSPACE, ENTRY_TABLE, KEYSPACE, READ_CONSISTENCY_LEVEL, READ_SERIAL_CONSISTENCY_LEVEL, WRITE_CONSISTENCY_LEVEL, WRITE_SERIAL_CONSISTENCY_LEVEL, REPLICATION_STRATEGY, COMPRESSION, SERVERS);
   }

   private final Attribute<Boolean> autoCreateKeyspace;
   private final Attribute<String> entryTable;
   private final Attribute<String> keyspace;
   private final Attribute<ConsistencyLevel> readConsistencyLevel;
   private final Attribute<ConsistencyLevel> readSerialConsistencyLevel;
   private final Attribute<ConsistencyLevel> writeConsistencyLevel;
   private final Attribute<ConsistencyLevel> writeSerialConsistencyLevel;
   private final Attribute<String> replicationStrategy;
   private final Attribute<String> compression;
   private final Attribute<List<CassandraStoreServerConfiguration>> servers;
   private final CassandraStoreConnectionPoolConfiguration connectionPool;

   public CassandraStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore,
                                      CassandraStoreConnectionPoolConfiguration connectionPool) {
      super(attributes, async, singletonStore);
      autoCreateKeyspace = attributes.attribute(AUTO_CREATE_KEYSPACE);
      entryTable = attributes.attribute(ENTRY_TABLE);
      keyspace = attributes.attribute(KEYSPACE);
      readConsistencyLevel = attributes.attribute(READ_CONSISTENCY_LEVEL);
      readSerialConsistencyLevel = attributes.attribute(READ_SERIAL_CONSISTENCY_LEVEL);
      writeConsistencyLevel = attributes.attribute(WRITE_CONSISTENCY_LEVEL);
      writeSerialConsistencyLevel = attributes.attribute(WRITE_SERIAL_CONSISTENCY_LEVEL);
      replicationStrategy = attributes.attribute(REPLICATION_STRATEGY);
      compression = attributes.attribute(COMPRESSION);
      servers = attributes.attribute(SERVERS);
      this.connectionPool = connectionPool;
   }

   public Boolean autoCreateKeyspace() {
      return autoCreateKeyspace.get();
   }

   public String entryTable() {
      return entryTable.get();
   }

   public String keyspace() {
      return keyspace.get();
   }

   public ConsistencyLevel readConsistencyLevel() {
      return readConsistencyLevel.get();
   }

   public ConsistencyLevel readSerialConsistencyLevel() {
      return readSerialConsistencyLevel.get();
   }

   public ConsistencyLevel writeConsistencyLevel() {
      return writeConsistencyLevel.get();
   }

   public ConsistencyLevel writeSerialConsistencyLevel() {
      return writeSerialConsistencyLevel.get();
   }

   public String replicationStrategy() {
      return replicationStrategy.get();
   }

   public String compression() {
      return compression.get();
   }

   public List<CassandraStoreServerConfiguration> servers() {
      return servers.get();
   }

   public CassandraStoreConnectionPoolConfiguration connectionPool() {
      return connectionPool;
   }

}