package org.infinispan.persistence.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.cassandra.configuration.CassandraStoreConfiguration;
import org.infinispan.persistence.cassandra.configuration.CassandraStoreConnectionPoolConfiguration;
import org.infinispan.persistence.cassandra.configuration.CassandraStoreServerConfiguration;
import org.infinispan.persistence.cassandra.logging.Log;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.Executor;

/**
 * A persistent <code>CacheStore</code> based on Apache Cassandra project. See http://cassandra.apache.org/
 *
 * @author Tristan Tarrant
 * @author Jakub Markos
 */
@ConfiguredBy(CassandraStoreConfiguration.class)
public class CassandraStore implements AdvancedLoadWriteStore {

   private static final Log log = LogFactory.getLog(CassandraStore.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private InitializationContext ctx;
   private CassandraStoreConfiguration configuration;
   private Cluster cluster;
   private Session session;
   private TimeService timeService;
   private StreamingMarshaller marshaller;
   private MarshalledEntryFactory marshalledEntryFactory;
   private ByteBufferFactory byteBufferFactory;

   private String entryTable;

   private PreparedStatement writeStatement;
   private PreparedStatement selectStatement;
   private PreparedStatement containsStatement;
   private PreparedStatement selectAllStatement;
   private PreparedStatement deleteStatement;
   private PreparedStatement sizeStatement;
   private PreparedStatement clearStatement;

   @Override
   public void init(InitializationContext initializationContext) {
      ctx = initializationContext;
      timeService = ctx.getTimeService();
      marshaller = ctx.getMarshaller();
      marshalledEntryFactory = ctx.getMarshalledEntryFactory();
      byteBufferFactory = ctx.getByteBufferFactory();
      configuration = ctx.getConfiguration();
   }

   @Override
   public void start() {
      try {
         PoolingOptions poolingOptions = new PoolingOptions();
         CassandraStoreConnectionPoolConfiguration poolConfig = configuration.connectionPool();
         poolingOptions.setPoolTimeoutMillis(poolConfig.poolTimeoutMillis());
         poolingOptions.setHeartbeatIntervalSeconds(poolConfig.heartbeatIntervalSeconds());
         poolingOptions.setIdleTimeoutSeconds(poolConfig.idleTimeoutSeconds());

         Cluster.Builder builder = Cluster.builder();
         if (configuration.useSsl()) {
            builder.withSSL();
         }
         if (!configuration.username().isEmpty()) {
            builder.withCredentials(configuration.username(), configuration.password());
            System.out.println("configuration.username() = " + configuration.username());
            System.out.println("configuration.password() = " + configuration.password());
         }
         builder.withPoolingOptions(poolingOptions);

         ArrayList<InetSocketAddress> servers = new ArrayList<>();
         for (CassandraStoreServerConfiguration cassandraStoreServerConfiguration : configuration.servers()) {
            servers.add(new InetSocketAddress(cassandraStoreServerConfiguration.host(), cassandraStoreServerConfiguration.port()));
         }
         builder.addContactPointsWithPorts(servers);

         cluster = builder.build();

         if (configuration.autoCreateKeyspace()) {
            createKeySpace();
         }
         session = cluster.connect(configuration.keyspace());
         entryTable = configuration.entryTable();
         writeStatement = session.prepare("INSERT INTO " + entryTable + " (key, value, metadata) VALUES (?, ?, ?) USING TTL ?");
         writeStatement.setConsistencyLevel(configuration.writeConsistencyLevel());
         writeStatement.setSerialConsistencyLevel(configuration.writeSerialConsistencyLevel());
         deleteStatement = session.prepare("DELETE FROM " + entryTable + " WHERE key=?");
         deleteStatement.setConsistencyLevel(configuration.writeConsistencyLevel());
         deleteStatement.setSerialConsistencyLevel(configuration.writeSerialConsistencyLevel());
         selectStatement = session.prepare("SELECT value, metadata FROM " + entryTable + " WHERE key=?");
         selectStatement.setConsistencyLevel(configuration.readConsistencyLevel());
         selectStatement.setSerialConsistencyLevel(configuration.readSerialConsistencyLevel());
         containsStatement = session.prepare("SELECT key FROM " + entryTable + " WHERE key=?");
         selectAllStatement = session.prepare("SELECT key, value, metadata FROM " + entryTable);
         selectAllStatement.setConsistencyLevel(configuration.readConsistencyLevel());
         selectAllStatement.setSerialConsistencyLevel(configuration.readSerialConsistencyLevel());
         sizeStatement = session.prepare("SELECT count(*) FROM " + entryTable);
         clearStatement = session.prepare("TRUNCATE " + entryTable);
      } catch (Exception e) {
         throw log.errorCommunicating(e);
      }
      entryTable = configuration.entryTable();

      log.debug("Cassandra cache store started.");
   }

   private void createKeySpace() {
      try (Session session = cluster.connect()) { // session without an associated keyspace
         Metadata clusterMetadata = cluster.getMetadata();
         boolean keyspaceExists = clusterMetadata.getKeyspace(configuration.keyspace()) != null;
         if (!keyspaceExists) {
            log.debug("Creating a keyspace " + configuration.keyspace());
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + configuration.keyspace() + " WITH replication = " +
                                  configuration.replicationStrategy() + ";");
         }
         boolean entryTableExists = clusterMetadata.getKeyspace(configuration.keyspace())
               .getTable(configuration.entryTable()) != null;
         if (!entryTableExists) {
            log.debug("Creating an entry table " + configuration.entryTable());
            session.execute("CREATE TABLE " + configuration.keyspace() + "." + configuration.entryTable() + " (" +
                                  "key blob PRIMARY KEY," +
                                  "value blob," +
                                  "metadata blob) WITH COMPRESSION = " + configuration.compression() + ";");
         }
      } catch (Exception e) {
         throw log.errorCreatingKeyspace(e);
      }
   }

   @Override
   public void write(MarshalledEntry marshalledEntry) {
      if (trace) log.tracef("Writing to Cassandra: %s", marshalledEntry);
      int ttl = 0;
      ByteBuffer metadata = null;
      if (marshalledEntry.getMetadata() != null && marshalledEntry.getMetadata().expiryTime() > -1) {
         long now = timeService.wallClockTime();
         long expireAt = marshalledEntry.getMetadata().expiryTime();
         ttl = (int) (expireAt - now) / 1000;
         metadata = ByteBuffer.wrap(marshalledEntry.getMetadataBytes().getBuf());
      }
      ByteBuffer key = ByteBuffer.wrap(marshalledEntry.getKeyBytes().getBuf());
      ByteBuffer value = ByteBuffer.wrap(marshalledEntry.getValueBytes().getBuf());

      try {
         session.execute(writeStatement.bind(key, value, metadata, ttl));
         if (trace) log.tracef("Stored: %s", marshalledEntry);
      } catch (Exception e) {
         throw log.errorWritingEntry(e);
      }
   }

   @Override
   public boolean delete(Object o) {
      if (trace) log.tracef("Deleting from Cassandra: %s", o);
      if (contains(o)) {
         try {
            session.execute(deleteStatement.bind(marshall(o)));
         } catch (Exception e) {
            throw log.errorDeletingEntry(e);
         }
         if (trace) log.tracef("Deleted: %s", o);
         return true;
      }
      return false;
   }

   @Override
   public MarshalledEntry load(Object o) {
      if (trace) log.tracef("Loading from Cassandra: %s", o);
      Row row;
      try {
         row = session.execute(selectStatement.bind(marshall(o))).one();
      } catch (Exception e) {
         throw log.errorLoadingEntry(e);
      }
      if (row == null) {
         return null;
      }
      byte[] valueBytes = row.getBytes(0).array();
      org.infinispan.commons.io.ByteBuffer valueBuffer = byteBufferFactory.newByteBuffer(valueBytes, 0, valueBytes.length);
      org.infinispan.commons.io.ByteBuffer metadataBuffer = null;
      if (row.getBytes(1) != null) {
         byte[] metadataBytes = row.getBytes(1).array();
         metadataBuffer = byteBufferFactory.newByteBuffer(metadataBytes, 0, metadataBytes.length);
      }
      MarshalledEntry marshalledEntry = marshalledEntryFactory.newMarshalledEntry(o, valueBuffer, metadataBuffer);
      if (trace) log.tracef("Loaded: %s", marshalledEntry);
      return marshalledEntry;

   }

   @Override
   public boolean contains(Object o) {
      if (trace) log.tracef("Cassandra contains? key: %s", o);
      boolean contains;
      try {
         contains = session.execute(containsStatement.bind(marshall(o))).one() != null;
         if (contains) {
            if (trace) log.tracef("Cassandra contains: %s", o);
            return true;
         }
      } catch (Exception e) {
         throw log.errorCommunicating(e);
      }
      return contains;
   }

   @Override
   public void stop() {
      cluster.close();
   }

   /**
    * the driver has auto-paging, so not all entries are loaded at once
    */
   @Override
   public void process(KeyFilter filter, CacheLoaderTask task, Executor executor, boolean fetchValue, boolean fetchMetadata) {
      TaskContextImpl taskContext = new TaskContextImpl();
      ResultSet rows = null;
      try {
         rows = session.execute(selectAllStatement.bind());
      } catch (Exception e) {
         throw log.errorCommunicating(e);
      }
      for (Row row : rows) {
         if (taskContext.isStopped())
            break;
         byte[] keyBytes = row.getBytes(0).array();
         Object key = unmarshall(keyBytes);
         if (filter == null || filter.accept(key)) {
            try {
               byte[] valueBytes = row.getBytes(1).array();
               byte[] metadataBytes = row.getBytes(2) != null ? row.getBytes(2).array() : null;
               org.infinispan.commons.io.ByteBuffer keyBuffer = byteBufferFactory.newByteBuffer(keyBytes, 0, keyBytes.length);
               org.infinispan.commons.io.ByteBuffer valueBuffer = byteBufferFactory.newByteBuffer(valueBytes, 0, valueBytes.length);
               org.infinispan.commons.io.ByteBuffer metadataBuffer = null;
               if (metadataBytes != null) {
                  metadataBuffer = byteBufferFactory.newByteBuffer(metadataBytes, 0, metadataBytes.length);
               }
               MarshalledEntry marshalledEntry = marshalledEntryFactory.newMarshalledEntry(keyBuffer, valueBuffer, metadataBuffer);
               if (marshalledEntry != null) {
                  task.processEntry(marshalledEntry, taskContext);
               }
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               return;
            }
         }
      }
   }

   @Override
   public int size() {
      int size = 0;
      try {
         size = (int) session.execute(sizeStatement.bind()).one().getLong(0);

      } catch (Exception e) {
         throw log.errorCommunicating(e);
      }
      if (trace) log.tracef("Size of Cassandra store: %d", size);
      return size;
   }

   @Override
   public void clear() {
      try {
         if (trace) log.trace("Clearing Cassandra store");
         session.execute(clearStatement.bind());
         if (trace) log.trace("Cleared Cassandra store");
      } catch (Exception e) {
         throw log.errorClearing(e);
      }
   }

   @Override
   public void purge(Executor threadPool, PurgeListener listener) {
      // ignored, entries are expired by Cassandra
   }
   
   private ByteBuffer marshall(Object o) {
      try {
         return ByteBuffer.wrap(marshaller.objectToByteBuffer(o));
      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }

   private Object unmarshall(byte[] bytes) {
      try {
         return marshaller.objectFromByteBuffer(bytes);
      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }

}
