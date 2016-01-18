package org.infinispan.persistence.cassandra;

import org.apache.cassandra.service.CassandraDaemon;

import java.io.IOException;

/**
 * This is a copy of org.apache.cassandra.service.EmbeddedCassandraService, making sure only 1 CassandraDaemon is
 * running. Starting and stopping the daemon for each test class didn't work.
 *
 * @author Jakub Markos
 */
public class SingleEmbeddedCassandraService {

   static CassandraDaemon cassandraDaemon;

   public synchronized static void start() throws IOException {
      if (cassandraDaemon != null) {
         return;
      }
      cassandraDaemon = new CassandraDaemon();
      cassandraDaemon.init(null);
      cassandraDaemon.start();
   }

}
