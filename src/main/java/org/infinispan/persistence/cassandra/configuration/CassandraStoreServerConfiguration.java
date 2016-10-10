package org.infinispan.persistence.cassandra.configuration;

public class CassandraStoreServerConfiguration {
   private final String host;
   private final int port;

   CassandraStoreServerConfiguration(String host, int port) {
      this.host = host;
      this.port = port;
   }

   public String host() {
      return host;
   }

   public int port() {
      return port;
   }

}
