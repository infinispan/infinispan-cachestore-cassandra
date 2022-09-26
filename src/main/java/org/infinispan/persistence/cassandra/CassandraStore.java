package org.infinispan.persistence.cassandra;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import io.reactivex.rxjava3.core.Flowable;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.persistence.Store;
import org.infinispan.commons.time.TimeService;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.cassandra.configuration.CassandraStoreConfiguration;
import org.infinispan.persistence.cassandra.configuration.CassandraStoreConnectionPoolConfiguration;
import org.infinispan.persistence.cassandra.configuration.CassandraStoreServerConfiguration;
import org.infinispan.persistence.cassandra.logging.Log;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.PersistenceException;
import org.jboss.logging.Logger;
import org.kohsuke.MetaInfServices;
import org.reactivestreams.Publisher;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

/**
 * A persistent <code>CacheStore</code> based on Apache Cassandra project. See http://cassandra.apache.org/
 *
 * @author Tristan Tarrant
 * @author Jakub Markos
 */
@MetaInfServices
@Store(shared = true)
@ConfiguredBy(CassandraStoreConfiguration.class)
public class CassandraStore<K, V> implements AdvancedLoadWriteStore<K, V> {
   private static final Log log = Logger.getMessageLogger(Log.class, CassandraStore.class.getName());
   private static final boolean trace = log.isTraceEnabled();

   private InitializationContext ctx;
   private CassandraStoreConfiguration configuration;
   private CqlSession session;

   private TimeService timeService;

   private PersistenceMarshaller marshaller;
   private MarshallableEntryFactory<K, V> marshallableEntryFactory;
   private ByteBufferFactory byteBufferFactory;

   private PreparedStatement writeStatement;
   private PreparedStatement writeStatementWithTtl;
   private PreparedStatement selectStatement;
   private PreparedStatement containsStatement;
   private PreparedStatement selectAllStatement;
   private PreparedStatement selectAllKeysStatement;
   private PreparedStatement deleteStatement;
   private PreparedStatement sizeStatement;
   private PreparedStatement clearStatement;

   @Override
   public void init(InitializationContext initializationContext) {
      ctx = initializationContext;
      timeService = ctx.getTimeService();
      marshaller = ctx.getPersistenceMarshaller();
      marshallableEntryFactory = ctx.getMarshallableEntryFactory();
      byteBufferFactory = ctx.getByteBufferFactory();
      configuration = ctx.getConfiguration();
   }

   @Override
   public void start() {
      try {
         CassandraStoreConnectionPoolConfiguration poolConfig = configuration.connectionPool();
         DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder()
               .withDuration(DefaultDriverOption.HEARTBEAT_INTERVAL, Duration.ofSeconds(poolConfig.heartbeatIntervalSeconds()))
               .withDuration(DefaultDriverOption.HEARTBEAT_TIMEOUT, Duration.ofMillis(poolConfig.heartbeatTimeoutMs()))
               .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, poolConfig.localSize())
               .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, poolConfig.remoteSize())
               .build();

         CqlSessionBuilder builder = CqlSession.builder();
         builder.withConfigLoader(configLoader);

         if (configuration.useSsl()) {
            builder.withSslContext(SSLContext.getDefault());
         }
         if (!configuration.username().isEmpty()) {
            builder.withAuthCredentials(configuration.username(), configuration.password());
         }

         for (CassandraStoreServerConfiguration cassandraStoreServerConfiguration : configuration.servers()) {
            builder.addContactPoint(new InetSocketAddress(cassandraStoreServerConfiguration.host(), cassandraStoreServerConfiguration.port()));
         }

         builder.withLocalDatacenter(configuration.localDatacenter());

         if (configuration.autoCreateKeyspace()) {
            try (CqlSession buildKeyspaceSession = builder.build()) {
               createKeySpace(buildKeyspaceSession);
            } catch (Exception e) {
               throw log.errorCreatingKeyspace(e);
            }

         }

         builder.withKeyspace(configuration.keyspace());
         session = builder.build();


         writeStatement = session.prepare(SimpleStatement.builder("INSERT INTO " + configuration.entryTable() + " (key, value, created, last_used, metadata, internal_metadata) VALUES (?, ?, ?, ?, ?, ?)")
               .setConsistencyLevel(configuration.writeConsistencyLevel())
               .setSerialConsistencyLevel(configuration.writeSerialConsistencyLevel())
               .build());

         writeStatementWithTtl = session.prepare(SimpleStatement.builder("INSERT INTO " + configuration.entryTable() + " (key, value, created, last_used, metadata, internal_metadata) VALUES (?, ?, ?, ?, ?, ?) USING TTL ?")
               .setConsistencyLevel(configuration.writeConsistencyLevel())
               .setSerialConsistencyLevel(configuration.writeSerialConsistencyLevel())
               .build());

         deleteStatement = session.prepare(SimpleStatement.builder("DELETE FROM " + configuration.entryTable() + " WHERE key=?")
               .setConsistencyLevel(configuration.writeConsistencyLevel())
               .setSerialConsistencyLevel(configuration.writeSerialConsistencyLevel())
               .build());

         selectStatement = session.prepare(SimpleStatement.builder("SELECT value, created, last_used, metadata, internal_metadata FROM " + configuration.entryTable() + " WHERE key=?")
               .setConsistencyLevel(configuration.readConsistencyLevel())
               .setSerialConsistencyLevel(configuration.readSerialConsistencyLevel())
               .build());

         containsStatement = session.prepare("SELECT key FROM " + configuration.entryTable() + " WHERE key=?");
         selectAllStatement = session.prepare(SimpleStatement.builder("SELECT key, value, created, last_used, metadata, internal_metadata FROM " + configuration.entryTable())
               .setConsistencyLevel(configuration.readConsistencyLevel())
               .setSerialConsistencyLevel(configuration.readSerialConsistencyLevel())
               .build());

         selectAllKeysStatement = session.prepare(SimpleStatement.builder("SELECT key FROM " + configuration.entryTable())
               .setConsistencyLevel(configuration.readConsistencyLevel())
               .setSerialConsistencyLevel(configuration.readSerialConsistencyLevel())
               .build());

         sizeStatement = session.prepare("SELECT count(*) FROM " + configuration.entryTable());
         clearStatement = session.prepare("TRUNCATE " + configuration.entryTable());
      } catch (Exception e) {
         throw log.errorCommunicating(e);
      }

      log.debug("Cassandra cache store started.");
   }

   private void createKeySpace(CqlSession buildKeyspaceSession) {
      Metadata clusterMetadata = buildKeyspaceSession.getMetadata();
      boolean keyspaceExists = clusterMetadata.getKeyspace(configuration.keyspace()).isPresent();
      if (!keyspaceExists) {
         log.debug("Creating a keyspace " + configuration.keyspace());
         buildKeyspaceSession.execute("CREATE KEYSPACE IF NOT EXISTS " + configuration.keyspace() + " WITH replication = " +
               configuration.replicationStrategy() + ";");
      }

      boolean entryTableExists = clusterMetadata.getKeyspace(configuration.keyspace())
            .flatMap(ks -> ks.getTable(configuration.entryTable()))
            .isPresent();

      if (!entryTableExists) {
         log.debug("Creating an entry table " + configuration.entryTable());
         String createTableQuery = "CREATE TABLE " + configuration.keyspace() + "." + configuration.entryTable() + " (" +
               "key blob PRIMARY KEY," +
               "value blob," +
               "created bigint," +
               "last_used bigint," +
               "metadata blob," +
               "internal_metadata blob)";

         if (configuration.compression() != null && !configuration.compression().trim().isEmpty()) {
            createTableQuery += " WITH COMPRESSION = " + configuration.compression() + ";";
         } else {
            createTableQuery += ";";
         }

         buildKeyspaceSession.execute(createTableQuery);
      }
   }

   @Override
   public void write(MarshallableEntry marshallableEntry) {
      if (trace) log.tracef("Writing to Cassandra: %s", marshallableEntry);
      int ttl = 0;
      ByteBuffer metadata = null;
      ByteBuffer internalMetadata = null;
      if (marshallableEntry.getMetadata() != null) {
         long now = timeService.wallClockTime();
         long expireAt = marshallableEntry.expiryTime();
         ttl = (int) (expireAt - now) / 1000;
         metadata = ByteBuffer.wrap(marshallableEntry.getMetadataBytes().getBuf());
      }

      if (marshallableEntry.getInternalMetadata() != null) {
         internalMetadata = ByteBuffer.wrap(marshallableEntry.getInternalMetadataBytes().getBuf());
      }

      org.infinispan.commons.io.ByteBuffer keyBytes = marshallableEntry.getKeyBytes();
      org.infinispan.commons.io.ByteBuffer valueBytes = marshallableEntry.getValueBytes();
      ByteBuffer key = ByteBuffer.wrap(Arrays.copyOfRange(keyBytes.getBuf(), keyBytes.getOffset(), keyBytes.getLength()));
      ByteBuffer value = ByteBuffer.wrap(Arrays.copyOfRange(valueBytes.getBuf(), valueBytes.getOffset(), valueBytes.getLength()));
      long created = marshallableEntry.created();
      long lastUsed = marshallableEntry.lastUsed();

      try {
         if (ttl > 0) {
            session.execute(writeStatementWithTtl.bind(key, value, created, lastUsed, metadata, internalMetadata, ttl));
         } else {
            session.execute(writeStatement.bind(key, value, created, lastUsed, metadata, internalMetadata));
         }

         if (trace) log.tracef("Stored: %s", marshallableEntry);
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
   public MarshallableEntry<K, V> loadEntry(Object key) {
      if (trace) log.tracef("Loading from Cassandra: %s", key);
      Row row;
      try {
         row = session.execute(selectStatement.bind(marshall(key))).one();
      } catch (Exception e) {
         throw log.errorLoadingEntry(e);
      }
      if (row == null) {
         return null;
      }
      byte[] valueBytes = row.getByteBuffer(0).array();
      org.infinispan.commons.io.ByteBuffer valueBuffer = byteBufferFactory.newByteBuffer(valueBytes, 0, valueBytes.length);
      org.infinispan.commons.io.ByteBuffer metadataBuffer = null;
      org.infinispan.commons.io.ByteBuffer internalMetadataBuffer = null;

      long created = row.getLong(1);
      long lastUsed = row.getLong(2);
      if (row.getByteBuffer(3) != null) {
         byte[] metadataBytes = row.getByteBuffer(3).array();
         metadataBuffer = byteBufferFactory.newByteBuffer(metadataBytes, 0, metadataBytes.length);
      }

      if (row.getByteBuffer(4) != null) {
         byte[] internalMetadataBytes = row.getByteBuffer(4).array();
         internalMetadataBuffer = byteBufferFactory.newByteBuffer(internalMetadataBytes, 0, internalMetadataBytes.length);
      }

      MarshallableEntry<K, V> marshallableEntry = marshallableEntryFactory.create(key, valueBuffer, metadataBuffer,
            internalMetadataBuffer,
            metadataBuffer == null ? -1 : created,
            metadataBuffer == null ? -1 : lastUsed);

      if (trace) log.tracef("Loaded: %s", marshallableEntry);
      return marshallableEntry;

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
      log.info("Try to stop CassandraStore ...");
      log.info("closing current session ...");
      try {
         log.info("closing cql-session ...");
         session.close();
         log.info("completed");
      } catch (Exception e) {
         log.warn("Problem with close Cassandra cluster", e);
      }
      log.info("CassandraStore stopped.");
   }

   private Flowable<Row> publishRows(PreparedStatement statement) {
      // Defer the creation so ResultSet is created per subscription
      return Flowable.defer(() -> {
         ResultSet rows;
         try {
            rows = session.execute(statement.bind());
         } catch (Exception e) {
            return Flowable.error(log.errorCommunicating(e));
         }
         return Flowable.fromIterable(rows);
      });
   }

   @Override
   public Flowable<K> publishKeys(Predicate<? super K> filter) {
      Flowable<K> keyFlowable = publishRows(selectAllKeysStatement).map(row -> unmarshall(row.getByteBuffer(0).array()));

      if (filter != null) {
         keyFlowable = keyFlowable.filter(filter::test);
      }
      return keyFlowable;
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> entryPublisher(Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
      if (!fetchValue && !fetchMetadata) {
         Flowable<K> keyFlowable = publishKeys(filter);
         return keyFlowable.map(key -> ctx.<K, V>getMarshallableEntryFactory().create(key, (V) null, null, null, -1, -1));
      }

      Flowable<Row> entryFlowable = publishRows(selectAllStatement);
      if (filter != null) {
         entryFlowable = entryFlowable.filter(row -> filter.test(unmarshall(row.getByteBuffer(0).array())));
      }

      return entryFlowable.map(row -> {
         byte[] keyBytes = row.getByteBuffer(0).array();
         byte[] valueBytes = row.getByteBuffer(1).array();
         byte[] metadataBytes = null;
         byte[] internalMetadataBytes = null;
         long created = row.getLong(2);
         long lastUsed = row.getLong(3);

         if (fetchMetadata) {
            if (row.getByteBuffer(4) != null) {
               metadataBytes = row.getByteBuffer(4).array();
            }

            if (row.getByteBuffer(5) != null) {
               internalMetadataBytes = row.getByteBuffer(5).array();
            }
         }
         org.infinispan.commons.io.ByteBuffer keyBuffer = byteBufferFactory.newByteBuffer(keyBytes, 0, keyBytes.length);
         org.infinispan.commons.io.ByteBuffer valueBuffer = null;
         if (fetchValue) {
            valueBuffer = byteBufferFactory.newByteBuffer(valueBytes, 0, valueBytes.length);
         }
         org.infinispan.commons.io.ByteBuffer metadataBuffer = null;
         org.infinispan.commons.io.ByteBuffer internalMetadataBuffer = null;
         if (metadataBytes != null) {
            metadataBuffer = byteBufferFactory.newByteBuffer(metadataBytes, 0, metadataBytes.length);
         }
         if (internalMetadataBytes != null) {
            internalMetadataBuffer = byteBufferFactory.newByteBuffer(internalMetadataBytes, 0, internalMetadataBytes.length);
         }
         return marshallableEntryFactory.create(keyBuffer, valueBuffer, metadataBuffer, internalMetadataBuffer,
               metadataBuffer == null ? -1 : created,
               metadataBuffer == null ? -1 : lastUsed);
      });
   }

   @Override
   public int size() {
      int size;
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

   private <T> T unmarshall(byte[] bytes) {
      try {
         return (T) marshaller.objectFromByteBuffer(bytes);
      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }

}
