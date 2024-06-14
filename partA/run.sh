#!/bin/bash

# Remove the output directories
hadoop fs -rm -r Output1
hadoop fs -rm -r Output2

# Run the Hadoop job
hadoop jar target/partA-1.0-SNAPSHOT.jar MatrixMultiplication Input Output1 Output2
