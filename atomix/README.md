# Atomix Database Binding for YCSB

This directory contains the YCSB binding for Atomix Database, a distributed transactional key-value store.

## Overview

The Atomix binding provides a transactional interface to Atomix Database through gRPC. Each operation (read, write, delete) is performed within a transaction context that is automatically managed by the client.

## Configuration

The following properties can be configured:

| Property | Default | Description |
|----------|---------|-------------|
| `atomix.frontend.address` | `localhost:50051` | The address of the Atomix frontend server |
| `atomix.namespace` | `ycsb` | The namespace to use for keyspaces |
| `atomix.keyspace` | `default` | The keyspace name to use for operations |

## Usage

### Basic Usage

```bash
# Load data
./bin/ycsb load atomix -P workloads/workloada -p atomix.frontend.address=localhost:50051

# Run workload
./bin/ycsb run atomix -P workloads/workloada -p atomix.frontend.address=localhost:50051
```

### Custom Configuration

Create a properties file (e.g., `atomix.properties`):

```properties
atomix.frontend.address=localhost:50051
atomix.namespace=ycsb
atomix.keyspace=test_keyspace
```

Then use it:

```bash
./bin/ycsb load atomix -P workloads/workloada -P atomix.properties
./bin/ycsb run atomix -P workloads/workloada -P atomix.properties
```

## Features

- **Transactional Operations**: All operations are performed within transactions
- **Automatic Transaction Management**: Transactions are automatically started, committed, and aborted
- **gRPC Communication**: Uses gRPC for efficient communication with the Atomix frontend
- **Keyspace Management**: Automatically creates keyspaces if they don't exist

## Limitations

- **Scan Operations**: Scan operations are not currently implemented (returns `NOT_IMPLEMENTED`)
- **Field Selection**: Only the first field in a multi-field operation is used
- **Transaction Scope**: Each operation uses its own transaction (no batching across operations)

## Building

To build the Atomix binding:

```bash
cd ycsb/atomix
mvn clean package
```

This will generate the protobuf classes and create the JAR file.

## Troubleshooting

### Connection Issues

If you encounter connection issues:

1. Ensure the Atomix frontend server is running
2. Check that the `atomix.frontend.address` property is correct
3. Verify network connectivity between the client and server

### Transaction Errors

If you encounter transaction errors:

1. Check the Atomix server logs for detailed error messages
2. Ensure the keyspace exists and is accessible
3. Verify that the namespace and keyspace names are correct

## Example Workload

Here's an example workload configuration for testing:

```properties
# Workload configuration
workload=site.ycsb.workloads.CoreWorkload
recordcount=1000
operationcount=10000

# Atomix configuration
atomix.frontend.address=localhost:50051
atomix.namespace=ycsb
atomix.keyspace=test_keyspace

# Operation distribution
readproportion=0.5
updateproportion=0.5
scanproportion=0
insertproportion=0
```

This configuration will perform 50% reads and 50% updates on 1000 records with 10,000 total operations. 