/**
 * Created by teklof on 27.4.16.
 */

"use strict";
const co = require("co")
  , request = require("co-request")
  , Promise = require("bluebird")
  , commander = require("commander")
  , logger = require("winston")
  , _ = require("lodash")
  , bytes = require("bytes")
  , inspect = _.partialRight(require("util").inspect, {depth: 4})
  , formatBytes = _.partialRight(bytes.format, {thousandsSeparator: ' '})
  ;

const EXIT_INVALID_PARAM = 1
  , EXIT_UNCAUGHT_ERR = 2
  , EXIT_REQUEST_ERROR = 3
  , EXIT_NO_METADATA = 4
  ;

function collect(val, memo) {
  memo.push(val);
  return memo;
}

//noinspection JSCheckFunctionSignatures
commander
  .version("0.0.1")
  .option("-b, --broker <host>", "Druid broker address (required)")
  .option("-c, --coordinator <host>", "Druid coordinator address (required)")
  .option("-i, --interval <list>", "ISO 8601 intervals to append. May be given multiple times (required)", collect, [])
  .option("-d, --data-source <dataSource>", "Druid data source to append (required)")
  .option("-r, --max-concurrent-reqs <n>", "Maximum concurrent requests [10]", 10)
  .option("-s, --max-segment-size <val>", "Maximum segment size [1GB]", "1GB")
  .option("-n, --dry-run", "Don't actually create the task")
  .option("-v, --verbose", "Be verbose")
  .parse(process.argv)
  ;

const brokerAddr = commander.broker
  , coordinatorAddr = commander.coordinator
  , dryRun = !!commander.dryRun
  , dataSource = commander.dataSource
  , intervals = commander.interval
  , maxConcurrent = commander.maxConcurrentReqs
  , maxSegmentSize = bytes.parse(commander.maxSegmentSize)
  ;

if(!(commander.broker && commander.coordinator && commander.dataSource && commander.interval)) {
  logger.error("missing command line parameter", {brokerAddr: brokerAddr, coordinatorAddr: coordinatorAddr, dataSource: dataSource, intervals: intervals});
  process.exit(EXIT_INVALID_PARAM);
}

if(commander.verbose) logger.level = "debug";

logger.info("Starting up", {coordinatorAddr: coordinatorAddr, brokerAddr: brokerAddr, noRun: dryRun, dataSource: dataSource, maxConcurrent: maxConcurrent, maxSegmentSize: formatBytes(maxSegmentSize)});

const getBasicMetadata = co.wrap(function* getBasicMetadata(query, broker) {
  logger.info("requesting basic segment metadata", {query: query, brokerAddr: broker});
  var resp;

  try {
    resp = yield request({
      uri: `http://${broker}/druid/v2/`
      , method: "POST"
      , headers: {"Content-Type": "application/json"}
      , json: query
    });
  } catch (e) {
    logger.error("error fetching basic metadata", {"err": e});
    process.exit(EXIT_REQUEST_ERROR);
  }

  let metadata = (resp.body || []).map(ob => ({id: ob.id, size: ob.size}));
  logger.info("got basic segment metadata", {segments: metadata.map(ob => ob.id)});
  return metadata;
});

const sizeOf = segments => segments.reduce((acc, obj) => acc + obj.size, 0);

const getFullMetadata = co.wrap(function* getFullMetadata(basicMetadata, dataSource, coordinator) {
  logger.info("getting full segment metadata from coordinator",
    {nSegments: basicMetadata.length, coordinatorAddr: coordinator});

  let fetchSegment = segment => {
    logger.debug("getting full metadata", {"segmentId": segment.id});
    return Promise.props(_.assign(segment, {
      metadata: Promise.resolve(
        request(`http://${coordinator}/druid/coordinator/v1/datasources/${dataSource}/segments/${segment.id}`))
        .get("body")
        .then(JSON.parse)
        .get("metadata")
    }));
  };

  let fullMetadata = yield Promise.map(basicMetadata, fetchSegment, {concurrency: maxConcurrent}).catch(e => {
    logger.error("error fetching full metadata", {"err": e});
    process.exit(EXIT_REQUEST_ERROR);
  });

  logger.info("got full metadatas", {
    "totalSize": formatBytes(sizeOf(fullMetadata.map(_.property("metadata"))))
  });

  return fullMetadata;
});

function toIndexingTask(dataSource, metadatas, index) {
  return {
    type: "append"
      , id: `append_${dataSource}_${new Date().toISOString()}_${index}`
    , dataSource: dataSource
    , segments: metadatas
  }
}

co(function*() {
  let metadataQuery = {
    "queryType": "segmentMetadata"
    , "dataSource": dataSource
    , "intervals": intervals
    , "toInclude": {"type": "none"}
  };

  let basicMetadata = yield getBasicMetadata(metadataQuery, brokerAddr);

  if(!basicMetadata.length) {
    logger.error("No basic metadata found for this source and interval", {"intervals": intervals, "dataSource": dataSource});
    process.exit(EXIT_NO_METADATA);
  }


  let fullMetadatas = yield getFullMetadata(basicMetadata, dataSource, coordinatorAddr);

  let chunkOb = fullMetadatas.reduce((acc, metadata) => {
    if (acc.currSize + metadata.metadata.size <= maxSegmentSize) {
      acc.currChunk.push(metadata.metadata);
      acc.currSize += metadata.metadata.size;
    } else {
      logger.debug("created new chunk", {chunkSize: formatBytes(acc.currSize)});
      acc.chunks.push(acc.currChunk);
      acc.currChunk = [];
      acc.currSize = 0;
    }

    return acc;
  }, {currSize: 0, currChunk: [], chunks: []});

  chunkOb.chunks.push(chunkOb.currChunk);
  let chunks = chunkOb.chunks;

  let tasks = chunks.map(_.partial(toIndexingTask, dataSource));
  logger.info("tasks created", {nTasks: tasks.length});

  _.each(tasks, task => logger.info({taskId: task.id, newSize: formatBytes(sizeOf(task.segments))}));

  if(dryRun) {
    logger.info(inspect(tasks));
    process.exit(0);
  }

}).catch((e) => {
  logger.error("Uncaught error", {err: e.toString()});
  process.exit(EXIT_UNCAUGHT_ERR);
});

