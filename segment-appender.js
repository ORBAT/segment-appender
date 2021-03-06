/**
 * Created by teklof on 27.4.16.
 */

"use strict";
const co = require("co")
  , request = require("requestretry")
  , Promise = require("bluebird")
  , commander = require("commander")
  , logger = require("winston")
  , _ = require("lodash")
  , bytes = require("bytes")
  , inspect = _.partialRight(require("util").inspect, {depth: 2})
  , formatBytes = _.partialRight(bytes.format, {thousandsSeparator: ' '})
  ;

const EXIT_INVALID_PARAM = 1
  , EXIT_UNCAUGHT_ERR = 2
  , EXIT_REQUEST_ERROR = 3
  , EXIT_NO_SEGMENTS = 4
  ;

function collect(val, memo) {
  memo.push(val);
  return memo;
}

logger.remove(logger.transports.Console);
logger.add(logger.transports.Console, {
  timestamp: function () {
    return new Date().toISOString();
  }
  , stderrLevels: ["debug", "info", "warn", "error"]
});

//noinspection JSCheckFunctionSignatures
commander
  .version("0.0.1")
  .option("-b, --broker <host>", "Druid broker address (required)")
  .option("-c, --coordinator <host>", "Druid coordinator address (required)")
  .option("-o, --overlord <host>", "Druid overlord address (required)")
  .option("-i, --interval <list>", "ISO 8601 intervals to append. May be given multiple times (required)", collect, [])
  .option("-d, --data-source <dataSource>", "Druid data source to append (required)")
  .option("-r, --max-concurrent-reqs <n>", "Maximum concurrent requests [10]", 10)
  .option("-x, --max-segment-size <val>", "Maximum segment size [900MB]", "900MB")
  .option("-m, --min-segment-size <val>", "Minimum segment size: smaller segments will not be created [400MB]", "400MB")
  .option("-n, --dry-run", "Don't actually create the task")
  .option("-v, --verbose", "Be verbose")
  .parse(process.argv)
  ;

const brokerAddr = commander.broker
  , coordinatorAddr = commander.coordinator
  , overlordAddr = commander.overlord
  , dryRun = !!commander.dryRun
  , dataSource = commander.dataSource
  , intervals = commander.interval
  , maxConcurrent = commander.maxConcurrentReqs
  , maxSegmentSize = bytes.parse(commander.maxSegmentSize)
  , minSegmentSize = bytes.parse(commander.minSegmentSize)
  ;

if(!(brokerAddr && coordinatorAddr && dataSource && intervals.length && overlordAddr)) {
  logger.error("missing command line parameter", {brokerAddr: brokerAddr, coordinatorAddr: coordinatorAddr, overlordAddr: overlordAddr, dataSource: dataSource, intervals: intervals});
  process.exit(EXIT_INVALID_PARAM);
}

if(commander.verbose) logger.level = "debug";

logger.info("Starting up", {coordinatorAddr: coordinatorAddr, brokerAddr: brokerAddr, overlordAddr: overlordAddr, dryRun: dryRun, dataSource: dataSource, maxConcurrent: maxConcurrent, maxSegmentSize: formatBytes(maxSegmentSize), minSegmentSize: formatBytes(minSegmentSize)});

const promiseFactory = resolver => new Promise(resolver);

const getBasicMetadata = co.wrap(function* getBasicMetadata(query, broker) {
  logger.info("requesting basic segment metadata", {query: query, brokerAddr: broker});
  var resp;

  try {
    resp = yield request({
      uri: `http://${broker}/druid/v2/`
      , method: "POST"
      , headers: {"Content-Type": "application/json"}
      , json: query
      , promiseFactory: promiseFactory
      , retryStrategy: request.RetryStrategies.HTTPOrNetworkError
    });
  } catch (e) {
    logger.error("error fetching basic metadata", {err: e.toString()});
    process.exit(EXIT_REQUEST_ERROR);
  }
  logger.debug("got response to metadata query", {body: resp.body});

  if(resp.body.error) {
    logger.error("error in segment metadata query", {err: resp.body.error});
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
    if(segment.id == "merged") {
      logger.info("not fetching metadata for 'merged' segment");
      return _.assign(segment, {metadata: {size: 0}});
    }

    return Promise.props(_.assign(segment, {
      metadata: Promise.resolve(
        request({
          uri: `http://${coordinator}/druid/coordinator/v1/datasources/${dataSource}/segments/${segment.id}`
          , promiseFactory: promiseFactory
          , retryStrategy: request.RetryStrategies.HTTPOrNetworkError
        }))
        .get("body")
        .then(JSON.parse)
        .get("metadata")
    }));
  };

  let fullMetadata = yield Promise.map(basicMetadata, fetchSegment, {concurrency: maxConcurrent}).catch(e => {
    logger.error("error fetching full metadata", {err: e.toString()});
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

function submitTasks(tasks, overlord) {
  let submitTask = task => {
    logger.info("submitting task", {"taskId": task.id, "nSegments": task.segments.length, size: formatBytes(sizeOf(task.segments)), overlordAddr: overlordAddr});
    return Promise.resolve(request({
        uri: `http://${overlord}/druid/indexer/v1/task`
        , promiseFactory: promiseFactory
        , headers: {"Content-Type": "application/json"}
        , method: "POST"
        , json: task
        , retryStrategy: request.RetryStrategies.HTTPOrNetworkError
      }))
      .get("body")
      .catch(e => {
        logger.error("error when submitting indexing task", {taskId: task.id, err: e.toString(), task: JSON.stringify(task, null, 2)})
      })
      ;
  };

  return Promise.map(tasks, submitTask, {concurrency: maxConcurrent});
}

co(function*() {
  let metadataQuery = {
    "queryType": "segmentMetadata"
    , "dataSource": dataSource
    , "intervals": intervals
    , "toInclude": {"type": "none"}
  };

  let basicMetadata = (yield getBasicMetadata(metadataQuery, brokerAddr));

  if(!basicMetadata.length) {
    logger.error("No segments found for this source and interval", {"intervals": intervals, "dataSource": dataSource});
    process.exit(EXIT_NO_SEGMENTS);
  }


  let fullMetadatas = yield getFullMetadata(basicMetadata, dataSource, coordinatorAddr);

  let getMetas = metadatas => _.map(metadatas, "metadata");
  let sizeOfChunk = chunk => sizeOf(getMetas(chunk));


  let endChunk = (acc, metadata) => {
    if(acc.currBytes < minSegmentSize || acc.currChunk.length == 1) {
      logger.info("chunk would be too small, skipping", {size: formatBytes(acc.currBytes), minSegmentSize: formatBytes(minSegmentSize), nSegments: acc.currChunk.length});
    } else {
      logger.debug("created new chunk", {chunkLength: acc.currChunk.length, chunkBytes: formatBytes(acc.currBytes)});
      acc.chunks.push(acc.currChunk);
    }

    acc.currChunk = [];
    acc.currBytes = 0;
    if(metadata && metadata.metadata.size > 0 && metadata.id != "merged") {
      acc.currChunk.push(metadata.metadata);
      acc.currBytes += metadata.metadata.size;
    }
  };

  let chunkOb = fullMetadatas.reduce((acc, metadata) => {
    const shardType = _.get(metadata, "metadata.shardSpec.type");
    if (shardType != "none") {
      logger.info("skipping segment with wrong shardSpec", {segmentId: metadata.id, shardSpec: JSON.stringify(_.get(metadata, "metadata.shardSpec"))});
      if(acc.currBytes != 0) {
        logger.info("terminating chunk early due to sharded segment");
        endChunk(acc);
      }
      return acc;
    }

    if(metadata.id == "merged" || metadata.metadata.size == 0) {
      logger.info("Found weird segment with id 'merged' or size 0, skipping", {segmentId: metadata.id,
        segmentSize: formatBytes(metadata.metadata.size)});

      if(acc.currBytes != 0) {
        logger.info("terminating chunk early due to 'merged' segment");
        endChunk(acc, metadata);
      }

      return acc;
    }


    if (acc.currBytes + metadata.metadata.size <= maxSegmentSize) {
      acc.currChunk.push(metadata.metadata);
      acc.currBytes += metadata.metadata.size;
    } else {
      endChunk(acc, metadata);
    }

    return acc;
  }, {currBytes: 0, currChunk: [], chunks: []});

  if(chunkOb.currBytes > minSegmentSize) {
    chunkOb.chunks.push(chunkOb.currChunk);
  } else if (chunkOb.currBytes > 0){
    logger.info("leftover segment would be too small, skipping", {size: formatBytes(chunkOb.currBytes), nSegments: chunkOb.currChunk.length});
  }

  let chunks = chunkOb.chunks;

  let tasks = chunks.map(_.partial(toIndexingTask, dataSource));
  logger.info("tasks created", {nTasks: tasks.length});

  if(dryRun) {
    _.each(tasks, task => logger.info({taskId: task.id, nSegments: task.segments.length, newSize: formatBytes(sizeOf(task.segments))}));
    // console.log(JSON.stringify(tasks, null, 2));
    process.exit(0);
  }

  let submits = yield submitTasks(tasks, overlordAddr);

  logger.info("tasks submitted");

  console.log(JSON.stringify(submits));

}).catch((e) => {
  logger.error("Uncaught error", {err: e.toString()});
  process.exit(EXIT_UNCAUGHT_ERR);
});

