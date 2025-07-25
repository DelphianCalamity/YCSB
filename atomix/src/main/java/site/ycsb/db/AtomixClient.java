/**
 * Copyright (c) 2012 - 2024 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import site.ycsb.db.atomix.AbortRequest;
import site.ycsb.db.atomix.AbortResponse;
import site.ycsb.db.atomix.CommitRequest;
import site.ycsb.db.atomix.CommitResponse;
import site.ycsb.db.atomix.CreateKeyspaceRequest;
import site.ycsb.db.atomix.CreateKeyspaceResponse;
import site.ycsb.db.atomix.DeleteRequest;
import site.ycsb.db.atomix.DeleteResponse;
import site.ycsb.db.atomix.FrontendGrpc;
import site.ycsb.db.atomix.GetRequest;
import site.ycsb.db.atomix.GetResponse;
import site.ycsb.db.atomix.Keyspace;
import site.ycsb.db.atomix.PutRequest;
import site.ycsb.db.atomix.PutResponse;
import site.ycsb.db.atomix.StartTransactionRequest;
import site.ycsb.db.atomix.StartTransactionResponse;
import site.ycsb.db.atomix.KeyRangeRequest;
import site.ycsb.db.atomix.Zone;
import site.ycsb.db.atomix.Region;
import com.google.protobuf.ByteString;

/**
 * A database interface layer for Atomix Database.
 * This client implements a transactional interface where each operation
 * is performed within a transaction context.
 */
public class AtomixClient extends DB {
  /** Logger for this class. */
  private static final Logger LOGGER = Logger.getLogger(
      AtomixClient.class.getName());

  // Configuration properties
  /** Property name for frontend address. */
  private static final String FRONTEND_ADDRESS_PROPERTY =
      "atomix.frontend.address";
  /** Default frontend address. */
  private static final String FRONTEND_ADDRESS_DEFAULT = "127.0.0.1:50057";
  /** Property name for namespace. */
  private static final String NAMESPACE_PROPERTY = "atomix.namespace";
  /** Default namespace. */
  private static final String NAMESPACE_DEFAULT = "ycsb";
  /** Property name for keyspace. */
  private static final String KEYSPACE_PROPERTY = "atomix.keyspace";
  /** Default keyspace. */
  private static final String KEYSPACE_DEFAULT = "default";
  /** Property name for number of keys. */
  private static final String NUM_KEYS_PROPERTY = "atomix.keyspace.num_keys";
  /** Default number of keys. */
  private static final String NUM_KEYS_DEFAULT = "1";

  // gRPC client
  /** gRPC frontend stub for communication. */
  private FrontendGrpc.FrontendBlockingStub frontendStub;
  /** gRPC managed channel. */
  private ManagedChannel channel;

  // Configuration
  /** Frontend server address. */
  private String frontendAddress;
  /** Database namespace. */
  private String namespace;
  /** Keyspace name. */
  private String keyspaceName;
  /** Keyspace object. */
  private Keyspace keyspace;
  /** Number of keys. */
  private int numKeys;
  // Transaction management
  /** Current transaction ID. */
  private String currentTransactionId;
  /** Map of thread names to transaction IDs. */
  private final Map<String, String> transactionIds = new ConcurrentHashMap<>();

  @Override
  public final void init() throws DBException {
    try {
      // Load configuration
      frontendAddress = getProperties().getProperty(FRONTEND_ADDRESS_PROPERTY,
          FRONTEND_ADDRESS_DEFAULT);
      namespace = getProperties().getProperty(NAMESPACE_PROPERTY,
          NAMESPACE_DEFAULT);
      keyspaceName = getProperties().getProperty(KEYSPACE_PROPERTY,
          KEYSPACE_DEFAULT);
      numKeys = Integer.parseInt(getProperties().getProperty(NUM_KEYS_PROPERTY,
          NUM_KEYS_DEFAULT));
      // Create keyspace
      keyspace = Keyspace.newBuilder()
        .setNamespace(namespace)
        .setName(keyspaceName)
        .build();

      // Create gRPC channel and stub
      channel = ManagedChannelBuilder.forTarget(frontendAddress)
        .usePlaintext()
        .build();
      frontendStub = FrontendGrpc.newBlockingStub(channel);

      // Create keyspace if it doesn't exist
      createKeyspaceIfNotExists();

      // INSERT_YOUR_CODE
      // Sleep for a short time to allow for keyspace creation to propagate
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }

      LOGGER.info("Atomix client initialized successfully");
      LOGGER.info("Frontend address: " + frontendAddress);
      LOGGER.info("Keyspace: " + namespace + "." + keyspaceName);

    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Failed to initialize Atomix client", e);
      throw new DBException("Failed to initialize Atomix client", e);
    }
  }

  @Override
  public final void cleanup() throws DBException {
    try {
      // Abort any open transaction
      if (currentTransactionId != null) {
        abortTransaction();
      }

      // Close gRPC channel
      if (channel != null && !channel.isShutdown()) {
        channel.shutdown();
      }

      LOGGER.info("Atomix client cleaned up successfully");
    } catch (Exception e) {
      LOGGER.log(Level.WARNING, "Error during cleanup", e);
    }
  }

  @Override
  public final Status read(final String table, final String key,
      final Set<String> fields, final Map<String, ByteIterator> result) {
    try {
      // Start transaction if not already started
      if (currentTransactionId == null) {
        startTransaction();
      }

      // Convert key to bytes
      String numberPart = key.replaceAll("\\D+", "");
      String formattedKey = numberPart;
      byte[] keyBytes = formattedKey.getBytes("UTF-8");
      LOGGER.info("Key: " + formattedKey);

      // Create get request
      GetRequest request = GetRequest.newBuilder()
            .setTransactionId(currentTransactionId)
            .setKeyspace(keyspace)
            .setKey(com.google.protobuf.ByteString.copyFrom(keyBytes))
            .build();

      // Execute get operation
      GetResponse response = frontendStub.get(request);

      if (response.getStatus().equals("Get request processed successfully")) {
        if (response.hasValue()) {
          // Convert response to ByteIterator
          byte[] valueBytes = response.getValue().toByteArray();
          result.put("field0", new ByteArrayByteIterator(valueBytes));
          return Status.OK;
        } else {
          return Status.OK;
        }
      } else {
        LOGGER.warning("Get operation failed: " + response.getStatus());
        return Status.ERROR;
      }

    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Error during read operation", e);
      return Status.ERROR;
    }
  }

  @Override
  public final Status scan(final String table, final String startkey,
      final int recordcount, final Set<String> fields,
      final Vector<HashMap<String, ByteIterator>> result) {
    // Atomix doesn't support scan operations in the current API
    // This would need to be implemented using multiple get operations
    LOGGER.warning("Scan operation not implemented for Atomix");
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public final Status update(final String table, final String key,
      final Map<String, ByteIterator> values) {
    try {
      // Start transaction if not already started
      if (currentTransactionId == null) {
        startTransaction();
      }

      // Convert key to bytes
      String numberPart = key.replaceAll("\\D+", "");
      String formattedKey = numberPart;
      byte[] keyBytes = formattedKey.getBytes("UTF-8");
      LOGGER.info("Key: " + formattedKey);

      // Convert values to bytes (take first field for simplicity)
      byte[] valueBytes = new byte[0];
      if (!values.isEmpty()) {
        ByteIterator firstValue = values.values().iterator().next();
        valueBytes = firstValue.toArray();
      }

      // Create put request
      PutRequest request = PutRequest.newBuilder()
            .setTransactionId(currentTransactionId)
            .setKeyspace(keyspace)
            .setKey(com.google.protobuf.ByteString.copyFrom(keyBytes))
            .setValue(com.google.protobuf.ByteString.copyFrom(valueBytes))
            .build();

      // Execute put operation
      PutResponse response = frontendStub.put(request);

      if (response.getStatus().equals("Put request processed successfully")) {

        // Commit transaction
        commitTransaction();
        
        return Status.OK;
      } else {
        LOGGER.warning("Put operation failed: " + response.getStatus());
        return Status.ERROR;
      }

    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Error during update operation", e);
      return Status.ERROR;
    }
  }

  @Override
  public final Status insert(final String table, final String key,
      final Map<String, ByteIterator> values) {
    // For Atomix, insert is the same as update
    return update(table, key, values);
  }

  @Override
  public final Status delete(final String table, final String key) {
    try {
      // Start transaction if not already started
      if (currentTransactionId == null) {
        startTransaction();
      }

      // Convert key to bytes
      byte[] keyBytes = key.getBytes("UTF-8");

      // Create delete request
      DeleteRequest request = DeleteRequest.newBuilder()
            .setTransactionId(currentTransactionId)
            .setKeyspace(keyspace)
            .setKey(com.google.protobuf.ByteString.copyFrom(keyBytes))
            .build();

      // Execute delete operation
      DeleteResponse response = frontendStub.delete(request);

      if (response.getStatus().equals(
          "Delete request processed successfully")) {
        return Status.OK;
      } else {
        LOGGER.warning("Delete operation failed: " + response.getStatus());
        return Status.ERROR;
      }

    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Error during delete operation", e);
      return Status.ERROR;
    }
  }

  /**
   * Start a new transaction.
   *
   * @throws Exception if transaction start fails
   */
  private void startTransaction() throws Exception {
    StartTransactionRequest request = StartTransactionRequest.newBuilder()
        .setKeyspace(keyspace)
        .build();

    StartTransactionResponse response = frontendStub.startTransaction(request);

    if (response.getStatus().equals(
        "Start transaction request processed successfully")) {
      currentTransactionId = response.getTransactionId();
      LOGGER.info("Current transaction ID: " + currentTransactionId);
      transactionIds.put(Thread.currentThread().getName(),
          currentTransactionId);
      LOGGER.fine("Started transaction: " + currentTransactionId);
    } else {
      throw new Exception("Failed to start transaction: "
          + response.getStatus());
    }
  }

  /**
   * Commit the current transaction.
   *
   * @throws Exception if transaction commit fails
   */
  private void commitTransaction() throws Exception {
    if (currentTransactionId == null) {
      return;
    }

    CommitRequest request = CommitRequest.newBuilder()
        .setTransactionId(currentTransactionId)
        .build();

    CommitResponse response = frontendStub.commit(request);

    if (response.getStatus().equals(
        "Commit request processed successfully")) {
      LOGGER.fine("Committed transaction: " + currentTransactionId);
      currentTransactionId = null;
    } else {
      throw new Exception("Failed to commit transaction: "
          + response.getStatus());
    }
  }

  /**
   * Abort the current transaction.
   *
   * @throws Exception if transaction abort fails
   */
  private void abortTransaction() throws Exception {
    if (currentTransactionId == null) {
      return;
    }

    AbortRequest request = AbortRequest.newBuilder()
        .setTransactionId(currentTransactionId)
        .build();

    AbortResponse response = frontendStub.abort(request);

    if (response.getStatus().equals(
        "Abort request processed successfully")) {
      LOGGER.fine("Aborted transaction: " + currentTransactionId);
      currentTransactionId = null;
    } else {
      throw new Exception("Failed to abort transaction: "
          + response.getStatus());
    }
  }

  /**
   * Create keyspace if it doesn't exist.
   *
   * @throws Exception if keyspace creation fails
   */
  private void createKeyspaceIfNotExists() throws Exception {
    List<KeyRangeRequest> ranges = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      ranges.add(KeyRangeRequest.newBuilder()
            .setLowerBoundInclusive(ByteString.copyFrom(new byte[] {(byte) i}))
            .setUpperBoundExclusive(ByteString.copyFrom(new byte[] {(byte) (i + 1)}))
            .build());
    }
    try {
      CreateKeyspaceRequest request = CreateKeyspaceRequest.newBuilder()
            .setNamespace(namespace)
            .setName(keyspaceName)
            .setPrimaryZone(Zone.newBuilder()
                .setRegion(Region.newBuilder()
                    // Do not set cloud (leave as default / unset)
                    .setName("test-region")
                    .build())
                .setName("a")
                .build())
            .addAllBaseKeyRanges(ranges)
            .build();

      CreateKeyspaceResponse response = frontendStub.createKeyspace(request);
      LOGGER.info("Created keyspace: " + response.getKeyspaceId());
    } catch (Exception e) {
      // Keyspace might already exist, which is fine
      LOGGER.fine("Keyspace creation failed (might already exist): "
          + e.getMessage());
    }
  }
}
