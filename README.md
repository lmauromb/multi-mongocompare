# Multi-Mongocompare

> Disclaimer: This software is provided "as is," without warranty of any kind.

Compare MongoDB nDocuments from two clusters.

Current behavior only compares using `countDocuments` and goes across every DB and Collection of the Source Cluster.

## Usage

```
go run main.go --sourceMongoURI="mongodb://localhost:27017/" --targetMongoURI="mongodb://localhost:27018/" --nWorkers=1 --outputFile="results.csv"
```

## Output 

Output is writted to a CSV file.

IE.

```csv
namespace, match, sourceCount, targetCount
myNewDb1.mySampleCollection,false,1,0
myNewDb2.mySampleCollection,false,1,0
myNewDb3.mySampleCollection,false,1,0
myNewDb5.mySampleCollection,false,1,0
myNewDb4.mySampleCollection,false,1,0
```

## Credits

Based on: https://github.com/fsnow/mongocompare