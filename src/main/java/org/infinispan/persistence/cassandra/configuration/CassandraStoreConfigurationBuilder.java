package org.infinispan.persistence.cassandra.configuration;

import com.datastax.driver.core.ConsistencyLevel;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

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
   public CassandraStoreConfigurationBuilder readConsistencyLevel(ConsistencyLevel readConsistencyLevel) {
      attributes.attribute(CassandraStoreConfiguration.READ_CONSISTENCY_LEVEL).set(readConsistencyLevel);
      return this;
   }

   @Override
   public CassandraStoreConfigurationBuilder readSerialConsistencyLevel(ConsistencyLevel readSerialConsistencyLevel) {
      attributes.attribute(CassandraStoreConfiguration.READ_SERIAL_CONSISTENCY_LEVEL).set(readSerialConsistencyLevel);
      return this;
   }

   @Override
   public CassandraStoreConfigurationBuilder writeConsistencyLevel(ConsistencyLevel writeConsistencyLevel) {
      attributes.attribute(CassandraStoreConfiguration.WRITE_CONSISTENCY_LEVEL).set(writeConsistencyLevel);
      return this;
   }

   @Override
   public CassandraStoreConfigurationBuilder writeSerialConsistencyLevel(ConsistencyLevel writeSerialConsistencyLevel) {
      attributes.attribute(CassandraStoreConfiguration.WRITE_SERIAL_CONSISTENCY_LEVEL).set(writeSerialConsistencyLevel);
      return this;
   }

   @Override
   public CassandraStoreConfigurationBuilder replicationStrategy(String replicationStrategy) {
      attributes.attribute(CassandraStoreConfiguration.REPLICATION_STRATEGY).set(replicationStrategy);
      return this;
   }

   @Override
   public CassandraStoreConfigurationBuilder compression(String compression) {
      attributes.attribute(CassandraStoreConfiguration.COMPRESSION).set(compression);
      return this;
   }

   @Override
   public CassandraStoreConfigurationBuilder useSsl(boolean useSsl) {
      attributes.attribute(CassandraStoreConfiguration.USE_SSL).set(useSsl);
      return this;
   }

   @Override
   public CassandraStoreConfigurationBuilder username(String username) {
      attributes.attribute(CassandraStoreConfiguration.USERNAME).set(username);
      return this;
   }

   @Override
   public CassandraStoreConfigurationBuilder password(String password) {
      attributes.attribute(CassandraStoreConfiguration.PASSWORD).set(password);
      return this;
   }

   @Override
   public CassandraStoreServerConfigurationBuilder addServer() {
      CassandraStoreServerConfigurationBuilder builder = new CassandraStoreServerConfigurationBuilder(this);
      this.servers.add(builder);
      return builder;
   }

   @Override
   public CassandraStoreConfigurationBuilder addProperty(String key, String value) {
      switch (key) {
         case "autoCreateKeyspace":
            return autoCreateKeyspace(Boolean.parseBoolean(value));
         case "keyspace":
            return keyspace(value);
         case "entryTable":
            return entryTable(value);
         case "readConsistencyLevel":
            return readConsistencyLevel(ConsistencyLevel.valueOf(value));
         case "readSerialConsistencyLevel":
            return readSerialConsistencyLevel(ConsistencyLevel.valueOf(value));
         case "writeConsistencyLevel":
            return writeConsistencyLevel(ConsistencyLevel.valueOf(value));
         case "writeSerialConsistencyLevel":
            return writeSerialConsistencyLevel(ConsistencyLevel.valueOf(value));
         case "replicationStrategy":
            return replicationStrategy(value);
         case "compression":
            return compression(value);
         case "useSsl":
            return useSsl(Boolean.parseBoolean(value));
         case "username":
            return username(value);
         case "password":
            return password(value);
         case "servers":
            String[] split = value.split(",");
            for (String s : split) {
               String host = s.substring(0, s.indexOf('['));
               addServer().host(host).port(Integer.parseInt(s.substring(s.indexOf('[') + 1, s.indexOf(']'))));
            }
            return this;
         case "connectionPool.heartbeatIntervalSeconds":
            connectionPool().heartbeatIntervalSeconds(Integer.parseInt(value));
            return this;
         case "connectionPool.idleTimeoutSeconds":
            connectionPool().idleTimeoutSeconds(Integer.parseInt(value));
            return this;
         case "connectionPool.poolTimeoutMillis":
            connectionPool().poolTimeoutMillis(Integer.parseInt(value));
            return this;
         default:
            throw new CacheConfigurationException("Couldn't find a configuration option named [" + key + "] in CassandraStore!");
      }
   }

   @Override
   public CassandraStoreConfigurationBuilder withProperties(Properties p) {
      for (Object key : p.keySet()) {
         addProperty((String) key, p.getProperty((String) key));
      }
      return this;
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