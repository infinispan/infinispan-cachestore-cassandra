package org.infinispan.persistence.cassandra.configuration;

import com.datastax.driver.core.ConsistencyLevel;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:jmarkos@redhat.com">Jakub Markos</a>
 */
public class CassandraStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<CassandraStoreConfiguration, CassandraStoreConfigurationBuilder>
      implements CassandraStoreConfigurationChildBuilder<CassandraStoreConfigurationBuilder> {

   private final CassandraStoreConnectionPoolConfigurationBuilder connectionPool;
   private List<CassandraStoreServerConfigurationBuilder> servers = new ArrayList<CassandraStoreServerConfigurationBuilder>();

   public CassandraStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, CassandraStoreConfiguration.attributeDefinitionSet());
      connectionPool = new CassandraStoreConnectionPoolConfigurationBuilder(this);
   }

   @Override
   public CassandraStoreConfigurationBuilder self() {
      return this;
   }

   @Override
   public CassandraStoreConnectionPoolConfigurationBuilder connectionPool() {
      return connectionPool;
   }

   @Override
   public CassandraStoreConfigurationBuilder autoCreateKeyspace(boolean autoCreateKeyspace) {
      attributes.attribute(CassandraStoreConfiguration.AUTO_CREATE_KEYSPACE).set(autoCreateKeyspace);
      return this;
   }

   @Override
   public CassandraStoreConfigurationBuilder entryTable(String entryTable) {
      attributes.attribute(CassandraStoreConfiguration.ENTRY_TABLE).set(entryTable);
      return this;
   }

   @Override
   public CassandraStoreConfigurationBuilder keyspace(String keyspace) {
      attributes.attribute(CassandraStoreConfiguration.KEYSPACE).set(keyspace);
      return this;
   }

   @Override
   public CassandraStoreConfigurationBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
      attributes.attribute(CassandraStoreConfiguration.CONSISTENCY_LEVEL).set(consistencyLevel);
      return this;
   }

   @Override
   public CassandraStoreConfigurationBuilder serialConsistencyLevel(ConsistencyLevel serialConsistencyLevel) {
      attributes.attribute(CassandraStoreConfiguration.SERIAL_CONSISTENCY_LEVEL).set(serialConsistencyLevel);
      return this;
   }

   @Override
   public CassandraStoreServerConfigurationBuilder addServer() {
      CassandraStoreServerConfigurationBuilder builder = new CassandraStoreServerConfigurationBuilder(this);
      this.servers.add(builder);
      return builder;
   }

   @Override
   public CassandraStoreConfiguration create() {
      List<CassandraStoreServerConfiguration> cassServers = new ArrayList<CassandraStoreServerConfiguration>();
      for (CassandraStoreServerConfigurationBuilder server : servers) {
         cassServers.add(server.create());
      }
      attributes.attribute(CassandraStoreConfiguration.SERVERS).set(cassServers);
      return new CassandraStoreConfiguration(attributes.protect(), async.create(), singletonStore.create(), connectionPool.create());
   }

   @Override
   public CassandraStoreConfigurationBuilder read(CassandraStoreConfiguration template) {
      super.read(template);
      this.connectionPool.read(template.connectionPool());
      for (CassandraStoreServerConfiguration server : template.servers()) {
         this.addServer().host(server.host()).port(server.port());
      }
      return this;
   }

   @Override
   public void validate() {
      this.connectionPool.validate();
      for (CassandraStoreServerConfigurationBuilder server : servers) {
         server.validate();
      }
   }

}