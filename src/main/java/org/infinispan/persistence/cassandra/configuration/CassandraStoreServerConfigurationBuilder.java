package org.infinispan.persistence.cassandra.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfiguration;

public class CassandraStoreServerConfigurationBuilder extends AbstractCassandraStoreConfigurationChildBuilder<CassandraStoreConfigurationBuilder>
      implements Builder<CassandraStoreServerConfiguration> {
   private String host = "127.0.0.1";
   private int port = 9042;

   CassandraStoreServerConfigurationBuilder(CassandraStoreConfigurationBuilder builder) {
      super(builder);
   }

   public CassandraStoreServerConfigurationBuilder host(String host) {
      this.host = host;
      return this;
   }

   public CassandraStoreServerConfigurationBuilder port(int port) {
      this.port = port;
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public CassandraStoreServerConfiguration create() {
      return new CassandraStoreServerConfiguration(host, port);
   }

   @Override
   public CassandraStoreServerConfigurationBuilder read(CassandraStoreServerConfiguration template) {
      this.host = template.host();
      this.port = template.port();

      return this;
   }

}