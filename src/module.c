/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#define REDISMODULE_MAIN

#include "module.h"

#include "RedisModulesSDK/redismodule.h"
#include "common.h"
#include "compaction.h"
#include "config.h"
#include "fast_double_parser_c/fast_double_parser_c.h"
#include "gears_commands.h"
#include "gears_integration.h"
#include "indexer.h"
#include "query_language.h"
#include "rdb.h"
#include "redisgears.h"
#include "reply.h"
#include "resultset.h"
#include "tsdb.h"
#include "version.h"

#include <ctype.h>
#include <limits.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include "rmutil/alloc.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"
#include <sys/ipc.h>
#include <sys/shm.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <math.h>

#ifndef REDISTIMESERIES_GIT_SHA
#define REDISTIMESERIES_GIT_SHA "unknown"
#endif

RedisModuleType *SeriesType;

int TSDB_info(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 2 || argc > 3) {
        return RedisModule_WrongArity(ctx);
    }

    Series *series;
    RedisModuleKey *key;
    int status = GetSeries(ctx, argv[1], &key, &series, REDISMODULE_READ);
    if (!status) {
        return REDISMODULE_ERR;
    }

    int is_debug = RMUtil_ArgExists("DEBUG", argv, argc, 1);
    if (is_debug) {
        RedisModule_ReplyWithArray(ctx, 13 * 2);
    } else {
        RedisModule_ReplyWithArray(ctx, 12 * 2);
    }

    long long skippedSamples;
    long long firstTimestamp = getFirstValidTimestamp(series, &skippedSamples);

    RedisModule_ReplyWithSimpleString(ctx, "totalSamples");
    RedisModule_ReplyWithLongLong(ctx, SeriesGetNumSamples(series) - skippedSamples);
    RedisModule_ReplyWithSimpleString(ctx, "memoryUsage");
    RedisModule_ReplyWithLongLong(ctx, SeriesMemUsage(series));
    RedisModule_ReplyWithSimpleString(ctx, "firstTimestamp");
    RedisModule_ReplyWithLongLong(ctx, firstTimestamp);
    RedisModule_ReplyWithSimpleString(ctx, "lastTimestamp");
    RedisModule_ReplyWithLongLong(ctx, series->lastTimestamp);
    RedisModule_ReplyWithSimpleString(ctx, "retentionTime");
    RedisModule_ReplyWithLongLong(ctx, series->retentionTime);
    RedisModule_ReplyWithSimpleString(ctx, "chunkCount");
    RedisModule_ReplyWithLongLong(ctx, RedisModule_DictSize(series->chunks));
    RedisModule_ReplyWithSimpleString(ctx, "chunkSize");
    RedisModule_ReplyWithLongLong(ctx, series->chunkSizeBytes);
    RedisModule_ReplyWithSimpleString(ctx, "chunkType");
    if (series->options & SERIES_OPT_UNCOMPRESSED) {
        RedisModule_ReplyWithSimpleString(ctx, "uncompressed");
    } else {
        RedisModule_ReplyWithSimpleString(ctx, "compressed");
    };
    RedisModule_ReplyWithSimpleString(ctx, "duplicatePolicy");
    if (series->duplicatePolicy != DP_NONE) {
        RedisModule_ReplyWithSimpleString(ctx, DuplicatePolicyToString(series->duplicatePolicy));
    } else {
        RedisModule_ReplyWithNull(ctx);
    }

    RedisModule_ReplyWithSimpleString(ctx, "labels");
    ReplyWithSeriesLabels(ctx, series);

    RedisModule_ReplyWithSimpleString(ctx, "sourceKey");
    if (series->srcKey == NULL) {
        RedisModule_ReplyWithNull(ctx);
    } else {
        RedisModule_ReplyWithString(ctx, series->srcKey);
    }

    RedisModule_ReplyWithSimpleString(ctx, "rules");
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    CompactionRule *rule = series->rules;
    int ruleCount = 0;
    while (rule != NULL) {
        RedisModule_ReplyWithArray(ctx, 3);
        RedisModule_ReplyWithString(ctx, rule->destKey);
        RedisModule_ReplyWithLongLong(ctx, rule->timeBucket);
        RedisModule_ReplyWithSimpleString(ctx, AggTypeEnumToString(rule->aggType));

        rule = rule->nextRule;
        ruleCount++;
    }
    RedisModule_ReplySetArrayLength(ctx, ruleCount);

    if (is_debug) {
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(series->chunks, ">", "", 0);
        Chunk_t *chunk = NULL;
        int chunkCount = 0;
        RedisModule_ReplyWithSimpleString(ctx, "Chunks");
        RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
        while (RedisModule_DictNextC(iter, NULL, (void *)&chunk)) {
            size_t chunkSize = series->funcs->GetChunkSize(chunk, FALSE);
            RedisModule_ReplyWithArray(ctx, 5 * 2);
            RedisModule_ReplyWithSimpleString(ctx, "startTimestamp");
            RedisModule_ReplyWithLongLong(ctx, series->funcs->GetFirstTimestamp(chunk));
            RedisModule_ReplyWithSimpleString(ctx, "endTimestamp");
            RedisModule_ReplyWithLongLong(ctx, series->funcs->GetLastTimestamp(chunk));
            RedisModule_ReplyWithSimpleString(ctx, "samples");
            u_int64_t numOfSamples = series->funcs->GetNumOfSample(chunk);
            RedisModule_ReplyWithLongLong(ctx, numOfSamples);
            RedisModule_ReplyWithSimpleString(ctx, "size");
            RedisModule_ReplyWithLongLong(ctx, chunkSize);
            RedisModule_ReplyWithSimpleString(ctx, "bytesPerSample");
            RedisModule_ReplyWithDouble(ctx, (float)chunkSize / numOfSamples);
            chunkCount++;
        }
        RedisModule_DictIteratorStop(iter);
        RedisModule_ReplySetArrayLength(ctx, chunkCount);
    }
    RedisModule_CloseKey(key);

    return REDISMODULE_OK;
}

void _TSDB_queryindex_impl(RedisModuleCtx *ctx, QueryPredicateList *queries) {
    RedisModuleDict *result = QueryIndex(ctx, queries->list, queries->count);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    char *currentKey;
    size_t currentKeyLen;
    long long replylen = 0;
    while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModule_ReplyWithStringBuffer(ctx, currentKey, currentKeyLen);
        replylen++;
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_ReplySetArrayLength(ctx, replylen);
}

int TSDB_queryindex(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }

    int query_count = argc - 1;

    int response = 0;
    QueryPredicateList *queries = parseLabelListFromArgs(ctx, argv, 1, query_count, &response);
    if (response == TSDB_ERROR) {
        QueryPredicateList_Free(queries);
        return RTS_ReplyGeneralError(ctx, "TSDB: failed parsing labels");
    }

    if (CountPredicateType(queries, EQ) + CountPredicateType(queries, LIST_MATCH) == 0) {
        QueryPredicateList_Free(queries);
        return RTS_ReplyGeneralError(ctx, "TSDB: please provide at least one matcher");
    }

    if (IsGearsLoaded()) {
        TSDB_queryindex_RG(ctx, queries);
        QueryPredicateList_Free(queries);
    } else {
        _TSDB_queryindex_impl(ctx, queries);
        QueryPredicateList_Free(queries);
    }

    return REDISMODULE_OK;
}

// multi-series groupby logic
static int replyGroupedMultiRange(RedisModuleCtx *ctx,
                                  TS_ResultSet *resultset,
                                  RedisModuleDict *result,
                                  MRangeArgs args) {
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    char *currentKey = NULL;
    size_t currentKeyLen;
    Series *series = NULL;

    while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModuleKey *key;
        const int status = SilentGetSeries(ctx,
                                           RedisModule_CreateString(ctx, currentKey, currentKeyLen),
                                           &key,
                                           &series,
                                           REDISMODULE_READ);
        if (!status) {
            RedisModule_Log(
                ctx, "warning", "couldn't open key or key is not a Timeseries. key=%s", currentKey);
            // The iterator may have been invalidated, stop and restart from after the current
            // key.
            RedisModule_DictIteratorStop(iter);
            iter = RedisModule_DictIteratorStartC(result, ">", currentKey, currentKeyLen);
            continue;
        }
        ResultSet_AddSerie(resultset, series, RedisModule_StringPtrLen(series->keyName, NULL));
        RedisModule_CloseKey(key);
    }
    RedisModule_DictIteratorStop(iter);

    // Apply the reducer
    ResultSet_ApplyReducer(resultset,
                           args.startTimestamp,
                           args.endTimestamp,
                           args.aggregationArgs.aggregationClass,
                           args.aggregationArgs.timeDelta,
                           args.count,
                           args.reverse,
                           args.gropuByReducerOp);

    // Do not apply the aggregation on the resultset, do apply max results on the final result
    replyResultSet(ctx,
                   resultset,
                   args.withLabels,
                   args.startTimestamp,
                   args.endTimestamp,
                   NULL,
                   0,
                   args.count,
                   args.reverse);

    ResultSet_Free(resultset);
    return REDISMODULE_OK;
}

// Previous multirange reply logic ( unchanged )
static int replyUngroupedMultiRange(RedisModuleCtx *ctx, RedisModuleDict *result, MRangeArgs args) {
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    char *currentKey;
    size_t currentKeyLen;
    long long replylen = 0;
    Series *series;
    while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModuleKey *key;
        const int status = SilentGetSeries(ctx,
                                           RedisModule_CreateString(ctx, currentKey, currentKeyLen),
                                           &key,
                                           &series,
                                           REDISMODULE_READ);

        if (!status) {
            RedisModule_Log(ctx,
                            "couldn't open key or key is not a Timeseries. key=%.*s",
                            currentKeyLen,
                            currentKey);
            // The iterator may have been invalidated, stop and restart from after the current key.
            RedisModule_DictIteratorStop(iter);
            iter = RedisModule_DictIteratorStartC(result, ">", currentKey, currentKeyLen);
            continue;
        }
        ReplySeriesArrayPos(ctx,
                            series,
                            args.withLabels,
                            args.startTimestamp,
                            args.endTimestamp,
                            args.aggregationArgs.aggregationClass,
                            args.aggregationArgs.timeDelta,
                            args.count,
                            args.reverse);
        replylen++;
        RedisModule_CloseKey(key);
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_ReplySetArrayLength(ctx, replylen);

    return REDISMODULE_OK;
}

int TSDB_generic_mrange(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool rev) {
    RedisModule_AutoMemory(ctx);

    MRangeArgs args;
    if (parseMRangeCommand(ctx, argv, argc, &args) != REDISMODULE_OK) {
        return REDISMODULE_OK;
    }
    args.reverse = rev;

    RedisModuleDict *resultSeries =
        QueryIndex(ctx, args.queryPredicates->list, args.queryPredicates->count);

    int result = REDISMODULE_OK;
    if (args.groupByLabel) {
        TS_ResultSet *resultset = ResultSet_Create();
        ResultSet_GroupbyLabel(resultset, args.groupByLabel);

        result = replyGroupedMultiRange(ctx, resultset, resultSeries, args);
    } else {
        result = replyUngroupedMultiRange(ctx, resultSeries, args);
    }

    MRangeArgs_Free(&args);
    return result;
}

int TSDB_mrange(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (IsGearsLoaded()) {
        return TSDB_mrange_RG(ctx, argv, argc, false);
    }
    
    return TSDB_generic_mrange(ctx, argv, argc, false);
}

int TSDB_mrevrange(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (IsGearsLoaded()) {
        return TSDB_mrange_RG(ctx, argv, argc, true);
    }
    return TSDB_generic_mrange(ctx, argv, argc, true);
}

int TSDB_generic_range(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool rev) {
    RedisModule_AutoMemory(ctx);
	
    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }
    Series *series;
    RedisModuleKey *key;
    const int status = GetSeries(ctx, argv[1], &key, &series, REDISMODULE_READ);
    if (!status) {
        return REDISMODULE_ERR;
    }

    api_timestamp_t start_ts, end_ts;
    api_timestamp_t time_delta = 0;
    if (parseRangeArguments(ctx, series, 2, argv, &start_ts, &end_ts) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    long long count = -1;
    if (parseCountArgument(ctx, argv, argc, &count) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    AggregationClass *aggObject = NULL;
    int aggregationResult = parseAggregationArgs(ctx, argv, argc, &time_delta, &aggObject);
    if (aggregationResult == TSDB_ERROR) {
        return REDISMODULE_ERR;
    }

    ReplySeriesRange(ctx, series, start_ts, end_ts, aggObject, time_delta, count, rev);

    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

int TSDB_range(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return TSDB_generic_range(ctx, argv, argc, false);
}

int TSDB_revrange(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return TSDB_generic_range(ctx, argv, argc, true);
}

static void handleCompaction(RedisModuleCtx *ctx,
                             Series *series,
                             CompactionRule *rule,
                             api_timestamp_t timestamp,
                             double value) {
    timestamp_t currentTimestamp = CalcWindowStart(timestamp, rule->timeBucket);

    if (rule->startCurrentTimeBucket == -1LL) {
        // first sample, lets init the startCurrentTimeBucket
        rule->startCurrentTimeBucket = currentTimestamp;
    }

    if (currentTimestamp > rule->startCurrentTimeBucket) {
        RedisModuleKey *key =
            RedisModule_OpenKey(ctx, rule->destKey, REDISMODULE_READ | REDISMODULE_WRITE);
        if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
            // key doesn't exist anymore and we don't do anything
            return;
        }
        Series *destSeries = RedisModule_ModuleTypeGetValue(key);

        double aggVal;
        if (rule->aggClass->finalize(rule->aggContext, &aggVal) == TSDB_OK) {
            SeriesAddSample(destSeries, rule->startCurrentTimeBucket, aggVal);
        }
        rule->aggClass->resetContext(rule->aggContext);
        rule->startCurrentTimeBucket = currentTimestamp;
        RedisModule_CloseKey(key);
    }
    rule->aggClass->appendValue(rule->aggContext, value);
}

static int internalAdd_TSDB_downsampling(RedisModuleCtx *ctx,
                       Series *series,
                       api_timestamp_t timestamp,
                       double value,
                       DuplicatePolicy dp_override) {
    timestamp_t lastTS = series->lastTimestamp;
    uint64_t retention = series->retentionTime;
    // ensure inside retention period.
    if (retention && timestamp < lastTS && retention < lastTS - timestamp) {
        RTS_ReplyGeneralError(ctx, "TSDB: Timestamp is older than retention");
        return REDISMODULE_ERR;
    }

    if (timestamp <= series->lastTimestamp && series->totalSamples != 0) {
        if (SeriesUpsertSample(series, timestamp, value, dp_override) != REDISMODULE_OK) {
            RTS_ReplyGeneralError(ctx,
                                  "TSDB: Error at upsert, update is not supported in BLOCK mode");
            return REDISMODULE_ERR;
        }
    } else {
        if (SeriesAddSample(series, timestamp, value) != REDISMODULE_OK) {
            RTS_ReplyGeneralError(ctx, "TSDB: Error at add");
            return REDISMODULE_ERR;
        }
        // handle compaction rules
        CompactionRule *rule = series->rules;
        while (rule != NULL) {
            handleCompaction(ctx, series, rule, timestamp, value);
            rule = rule->nextRule;
        }
    }
    //RedisModule_ReplyWithLongLong(ctx, timestamp);
    return REDISMODULE_OK;
}

static int internalAdd(RedisModuleCtx *ctx,
                       Series *series,
                       api_timestamp_t timestamp,
                       double value,
                       DuplicatePolicy dp_override) {
    timestamp_t lastTS = series->lastTimestamp;
    uint64_t retention = series->retentionTime;
    // ensure inside retention period.
    if (retention && timestamp < lastTS && retention < lastTS - timestamp) {
        RTS_ReplyGeneralError(ctx, "TSDB: Timestamp is older than retention");
        return REDISMODULE_ERR;
    }

    if (timestamp <= series->lastTimestamp && series->totalSamples != 0) {
        if (SeriesUpsertSample(series, timestamp, value, dp_override) != REDISMODULE_OK) {
            RTS_ReplyGeneralError(ctx,
                                  "TSDB: Error at upsert, update is not supported in BLOCK mode");
            return REDISMODULE_ERR;
        }
    } else {
        if (SeriesAddSample(series, timestamp, value) != REDISMODULE_OK) {
            RTS_ReplyGeneralError(ctx, "TSDB: Error at add");
            return REDISMODULE_ERR;
        }
        // handle compaction rules
        CompactionRule *rule = series->rules;
        while (rule != NULL) {
            handleCompaction(ctx, series, rule, timestamp, value);
            rule = rule->nextRule;
        }
    }
    RedisModule_ReplyWithLongLong(ctx, timestamp);
    return REDISMODULE_OK;
}

static inline int add(RedisModuleCtx *ctx,
                      RedisModuleString *keyName,
                      RedisModuleString *timestampStr,
                      RedisModuleString *valueStr,
                      RedisModuleString **argv,
                      int argc) {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);
    double value;
    const char *valueCStr = RedisModule_StringPtrLen(valueStr, NULL);
    if ((fast_double_parser_c_parse_number(valueCStr, &value) == NULL))
        return RTS_ReplyGeneralError(ctx, "TSDB: invalid value");

    long long timestampValue;
    if ((RedisModule_StringToLongLong(timestampStr, &timestampValue) != REDISMODULE_OK)) {
        // if timestamp is "*", take current time (automatic timestamp)
        if (RMUtil_StringEqualsC(timestampStr, "*"))
            timestampValue = RedisModule_Milliseconds();
        else
            return RTS_ReplyGeneralError(ctx, "TSDB: invalid timestamp");
    }

    if (timestampValue < 0) {
        return RTS_ReplyGeneralError(ctx, "TSDB: invalid timestamp, must be positive number");
    }
    api_timestamp_t timestamp = (u_int64_t)timestampValue;

    Series *series = NULL;
    DuplicatePolicy dp = DP_NONE;

    if (argv != NULL && RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        // the key doesn't exist, lets check we have enough information to create one
        CreateCtx cCtx = { 0 };
        if (parseCreateArgs(ctx, argv, argc, &cCtx) != REDISMODULE_OK) {
            return REDISMODULE_ERR;
        }

        CreateTsKey(ctx, keyName, &cCtx, &series, &key);
        SeriesCreateRulesFromGlobalConfig(ctx, keyName, series, cCtx.labels, cCtx.labelsCount);
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType) {
        return RTS_ReplyGeneralError(ctx, "TSDB: the key is not a TSDB key");
    } else {
        series = RedisModule_ModuleTypeGetValue(key);
        //  overwride key and database configuration for DUPLICATE_POLICY
        if (argv != NULL &&
            ParseDuplicatePolicy(ctx, argv, argc, TS_ADD_DUPLICATE_POLICY_ARG, &dp) != TSDB_OK) {
            return REDISMODULE_ERR;
        }
    }
    int rv = internalAdd(ctx, series, timestamp, value, dp);
    RedisModule_CloseKey(key);
    return rv;
}

static inline int add_TSDB_downsampling(RedisModuleCtx *ctx,
                      RedisModuleString *keyName,
                      RedisModuleString *timestampStr,
                      RedisModuleString *valueStr,
                      RedisModuleString **argv,
                      int argc) {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);
    double value;
    const char *valueCStr = RedisModule_StringPtrLen(valueStr, NULL);
    if ((fast_double_parser_c_parse_number(valueCStr, &value) == NULL))
        return RTS_ReplyGeneralError(ctx, "TSDB: invalid value");

    long long timestampValue;
    if ((RedisModule_StringToLongLong(timestampStr, &timestampValue) != REDISMODULE_OK)) {
        // if timestamp is "*", take current time (automatic timestamp)
        if (RMUtil_StringEqualsC(timestampStr, "*"))
            timestampValue = RedisModule_Milliseconds();
        else
            return RTS_ReplyGeneralError(ctx, "TSDB: invalid timestamp");
    }

    if (timestampValue < 0) {
        return RTS_ReplyGeneralError(ctx, "TSDB: invalid timestamp, must be positive number");
    }
    api_timestamp_t timestamp = (u_int64_t)timestampValue;

    Series *series = NULL;
    DuplicatePolicy dp = DP_NONE;

    if (argv != NULL && RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        // the key doesn't exist, lets check we have enough information to create one
        CreateCtx cCtx = { 0 };
        if (parseCreateArgs(ctx, argv, argc, &cCtx) != REDISMODULE_OK) {
            return REDISMODULE_ERR;
        }

        CreateTsKey(ctx, keyName, &cCtx, &series, &key);
        SeriesCreateRulesFromGlobalConfig(ctx, keyName, series, cCtx.labels, cCtx.labelsCount);
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType) {
        return RTS_ReplyGeneralError(ctx, "TSDB: the key is not a TSDB key");
    } else {
        series = RedisModule_ModuleTypeGetValue(key);
        //  overwride key and database configuration for DUPLICATE_POLICY
        if (argv != NULL &&
            ParseDuplicatePolicy(ctx, argv, argc, TS_ADD_DUPLICATE_POLICY_ARG, &dp) != TSDB_OK) {
            return REDISMODULE_ERR;
        }
    }
    int rv = internalAdd_TSDB_downsampling(ctx, series, timestamp, value, dp);
    RedisModule_CloseKey(key);
    return rv;
}

int TSDB_madd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4 || (argc - 1) % 3 != 0) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModule_ReplyWithArray(ctx, (argc - 1) / 3);
    for (int i = 1; i < argc; i += 3) {
        RedisModuleString *keyName = argv[i];
        RedisModuleString *timestampStr = argv[i + 1];
        RedisModuleString *valueStr = argv[i + 2];
        add(ctx, keyName, timestampStr, valueStr, NULL, -1);
    }
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

int TSDB_add(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *keyName = argv[1];
    RedisModuleString *timestampStr = argv[2];
    RedisModuleString *valueStr = argv[3];

    int result = add(ctx, keyName, timestampStr, valueStr, argv, argc);
    RedisModule_ReplicateVerbatim(ctx);
    return result;
}

int CreateTsKey(RedisModuleCtx *ctx,
                RedisModuleString *keyName,
                CreateCtx *cCtx,
                Series **series,
                RedisModuleKey **key) {
    if (*key == NULL) {
        *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);
    }

    RedisModule_RetainString(ctx, keyName);
    *series = NewSeries(keyName, cCtx);
    if (RedisModule_ModuleTypeSetValue(*key, SeriesType, *series) == REDISMODULE_ERR) {
        return TSDB_ERROR;
    }

    IndexMetric(ctx, keyName, (*series)->labels, (*series)->labelsCount);

    return TSDB_OK;
}

int TSDB_create(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }

    Series *series;
    RedisModuleString *keyName = argv[1];
    CreateCtx cCtx = { 0 };
    if (parseCreateArgs(ctx, argv, argc, &cCtx) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);

    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(key);
        return RTS_ReplyGeneralError(ctx, "TSDB: key already exists");
    }

    CreateTsKey(ctx, keyName, &cCtx, &series, &key);
    RedisModule_CloseKey(key);

    RedisModule_Log(ctx, "verbose", "created new series");
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

int TSDB_alter(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }

    Series *series;
    RedisModuleKey *key;
    RedisModuleString *keyName = argv[1];
    CreateCtx cCtx = { 0 };
    if (parseCreateArgs(ctx, argv, argc, &cCtx) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    const int status = GetSeries(ctx, argv[1], &key, &series, REDISMODULE_READ | REDISMODULE_WRITE);
    if (!status) {
        return REDISMODULE_ERR;
    }
    if (RMUtil_ArgIndex("RETENTION", argv, argc) > 0) {
        series->retentionTime = cCtx.retentionTime;
    }

    if (RMUtil_ArgIndex("CHUNK_SIZE", argv, argc) > 0) {
        series->chunkSizeBytes = cCtx.chunkSizeBytes;
    }

    if (RMUtil_ArgIndex("DUPLICATE_POLICY", argv, argc) > 0) {
        series->duplicatePolicy = cCtx.duplicatePolicy;
    }

    if (RMUtil_ArgIndex("LABELS", argv, argc) > 0) {
        RemoveIndexedMetric(ctx, keyName, series->labels, series->labelsCount);
        // free current labels
        FreeLabels(series->labels, series->labelsCount);

        // set new newLabels
        series->labels = cCtx.labels;
        series->labelsCount = cCtx.labelsCount;
        IndexMetric(ctx, keyName, series->labels, series->labelsCount);
    }
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

/*
TS.DELETERULE SOURCE_KEY DEST_KEY
 */
int TSDB_deleteRule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *srcKeyName = argv[1];

    // First try to remove the rule from the source key
    Series *srcSeries;
    RedisModuleKey *srcKey;
    const int statusS =
        GetSeries(ctx, srcKeyName, &srcKey, &srcSeries, REDISMODULE_READ | REDISMODULE_WRITE);
    if (!statusS) {
        return REDISMODULE_ERR;
    }

    RedisModuleString *destKeyName = argv[2];
    if (!SeriesDeleteRule(srcSeries, destKeyName)) {
        return RTS_ReplyGeneralError(ctx, "TSDB: compaction rule does not exist");
    }

    // If succeed to remove the rule from the source key remove from the destination too
    Series *destSeries;
    RedisModuleKey *destKey;
    const int statusD =
        GetSeries(ctx, destKeyName, &destKey, &destSeries, REDISMODULE_READ | REDISMODULE_WRITE);
    if (!statusD) {
        return REDISMODULE_ERR;
    }
    SeriesDeleteSrcRule(destSeries, srcKeyName);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_CloseKey(srcKey);
    RedisModule_CloseKey(destKey);
    return REDISMODULE_OK;
}

/*
TS.CREATERULE sourceKey destKey AGGREGATION aggregationType timeBucket
*/
int TSDB_createRule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 6) {
        return RedisModule_WrongArity(ctx);
    }

    // Validate aggregation arguments
    api_timestamp_t timeBucket;
    int aggType;
    const int result = _parseAggregationArgs(ctx, argv, argc, &timeBucket, &aggType);
    if (result == TSDB_NOTEXISTS) {
        return RedisModule_WrongArity(ctx);
    }
    if (result == TSDB_ERROR) {
        return REDISMODULE_ERR;
    }

    RedisModuleString *srcKeyName = argv[1];
    RedisModuleString *destKeyName = argv[2];
    if (!RedisModule_StringCompare(srcKeyName, destKeyName)) {
        return RTS_ReplyGeneralError(
            ctx, "TSDB: the source key and destination key should be different");
    }

    // First we verify the source is not a destination
    Series *srcSeries;
    RedisModuleKey *srcKey;
    const int statusS =
        GetSeries(ctx, srcKeyName, &srcKey, &srcSeries, REDISMODULE_READ | REDISMODULE_WRITE);
    if (!statusS) {
        return REDISMODULE_ERR;
    }
    if (srcSeries->srcKey) {
        return RTS_ReplyGeneralError(ctx, "TSDB: the source key already has a source rule");
    }

    // Second verify the destination doesn't have other rule
    Series *destSeries;
    RedisModuleKey *destKey;
    const int statusD =
        GetSeries(ctx, destKeyName, &destKey, &destSeries, REDISMODULE_READ | REDISMODULE_WRITE);
    if (!statusD) {
        return REDISMODULE_ERR;
    }
    srcKeyName = RedisModule_CreateStringFromString(ctx, srcKeyName);
    if (!SeriesSetSrcRule(destSeries, srcKeyName)) {
        return RTS_ReplyGeneralError(ctx, "TSDB: the destination key already has a rule");
    }
    RedisModule_RetainString(ctx, srcKeyName);

    // Last add the rule to source
    destKeyName = RedisModule_CreateStringFromString(ctx, destKeyName);
    if (SeriesAddRule(srcSeries, destKeyName, aggType, timeBucket) == NULL) {
        RedisModule_ReplyWithSimpleString(ctx, "TSDB: ERROR creating rule");
        return REDISMODULE_ERR;
    }
    RedisModule_RetainString(ctx, destKeyName);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_CloseKey(srcKey);
    RedisModule_CloseKey(destKey);
    return REDISMODULE_OK;
}

/*
TS.INCRBY ts_key NUMBER [TIMESTAMP timestamp]
*/
int TSDB_incrby(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *keyName = argv[1];
    Series *series;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        // the key doesn't exist, lets check we have enough information to create one
        CreateCtx cCtx = { 0 };
        if (parseCreateArgs(ctx, argv, argc, &cCtx) != REDISMODULE_OK) {
            return REDISMODULE_ERR;
        }

        CreateTsKey(ctx, keyName, &cCtx, &series, &key);
        SeriesCreateRulesFromGlobalConfig(ctx, keyName, series, cCtx.labels, cCtx.labelsCount);
    }

    series = RedisModule_ModuleTypeGetValue(key);

    double incrby = 0;
    if (RMUtil_ParseArgs(argv, argc, 2, "d", &incrby) != REDISMODULE_OK) {
        return RTS_ReplyGeneralError(ctx, "TSDB: invalid increase/decrease value");
    }

    long long currentUpdatedTime = -1;
    int timestampLoc = RMUtil_ArgIndex("TIMESTAMP", argv, argc);
    if (timestampLoc == -1 || RMUtil_StringEqualsC(argv[timestampLoc + 1], "*")) {
        currentUpdatedTime = RedisModule_Milliseconds();
    } else if (RedisModule_StringToLongLong(argv[timestampLoc + 1],
                                            (long long *)&currentUpdatedTime) != REDISMODULE_OK) {
        return RTS_ReplyGeneralError(ctx, "TSDB: invalid timestamp");
    }

    if (currentUpdatedTime < series->lastTimestamp && series->lastTimestamp != 0) {
        return RedisModule_ReplyWithError(
            ctx, "TSDB: for incrby/decrby, timestamp should be newer than the lastest one");
    }

    double result = series->lastValue;
    RMUtil_StringToLower(argv[0]);
    if (RMUtil_StringEqualsC(argv[0], "ts.incrby")) {
        result += incrby;
    } else {
        result -= incrby;
    }

    int rv = internalAdd(ctx, series, currentUpdatedTime, result, DP_LAST);
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_CloseKey(key);
    return rv;
}

int TSDB_get(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    Series *series;
    RedisModuleKey *key;
    const int status = GetSeries(ctx, argv[1], &key, &series, REDISMODULE_READ);
    if (!status) {
        return REDISMODULE_ERR;
    }

    ReplyWithSeriesLastDatapoint(ctx, series);
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

int TSDB_mget(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (IsGearsLoaded()) {
        return TSDB_mget_RG(ctx, argv, argc);
    }

    RedisModule_AutoMemory(ctx);

    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    int filter_location = RMUtil_ArgIndex("FILTER", argv, argc);
    if (filter_location == -1) {
        return RedisModule_WrongArity(ctx);
    }
    size_t query_count = argc - 1 - filter_location;
    const int withlabels_location = RMUtil_ArgIndex("WITHLABELS", argv, argc);
    int response = 0;
    QueryPredicateList *queries =
        parseLabelListFromArgs(ctx, argv, filter_location + 1, query_count, &response);
    if (response == TSDB_ERROR) {
        QueryPredicateList_Free(queries);
        return RTS_ReplyGeneralError(ctx, "TSDB: failed parsing labels");
    }

    if (CountPredicateType(queries, EQ) + CountPredicateType(queries, LIST_MATCH) == 0) {
        QueryPredicateList_Free(queries);
        return RTS_ReplyGeneralError(ctx, "TSDB: please provide at least one matcher");
    }

    RedisModuleDict *result = QueryIndex(ctx, queries->list, queries->count);
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    char *currentKey;
    size_t currentKeyLen;
    long long replylen = 0;
    Series *series;
    while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModuleKey *key;
        const int status = SilentGetSeries(ctx,
                                           RedisModule_CreateString(ctx, currentKey, currentKeyLen),
                                           &key,
                                           &series,
                                           REDISMODULE_READ);
        if (!status) {
            RedisModule_Log(ctx,
                            "warning",
                            "couldn't open key or key is not a Timeseries. key=%.*s",
                            currentKeyLen,
                            currentKey);
            continue;
        }
        RedisModule_ReplyWithArray(ctx, 3);
        RedisModule_ReplyWithStringBuffer(ctx, currentKey, currentKeyLen);
        if (withlabels_location >= 0) {
            ReplyWithSeriesLabels(ctx, series);
        } else {
            RedisModule_ReplyWithArray(ctx, 0);
        }
        ReplyWithSeriesLastDatapoint(ctx, series);
        replylen++;
        RedisModule_CloseKey(key);
    }
    RedisModule_ReplySetArrayLength(ctx, replylen);
    RedisModule_DictIteratorStop(iter);
    QueryPredicateList_Free(queries);
    return REDISMODULE_OK;
}

int TSDB_help(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	char *msg = "TS.train\nTS.predict\nTS.plot\nTS.downsampling\nTS.load\nTS.analysisi\n";
	RedisModule_ReplyWithSimpleString(ctx, msg);
	return REDISMODULE_OK;
}

#include "series_iterator.h"

int TSDB_plot(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    char plot_filename[100000];
    char tmp_filename[100000];
    char redisString[100000];
    char function[100000];
    // What is len used for ?
    size_t len;
    int ReplyLen = 0;
    char command[1000000];
    // Segmentation Fault
    // double array[500000];
    double array[250000];
	int arraylen = 0;
    // TS.PLOT key function [JPG jpg]
    // function : data, acf, pacf, diff_acf, diff_pacf, smooth, downsampling
    if (argc < 3 || argc == 4 || argc > 5) {
        return RedisModule_WrongArity(ctx);
    } else if (argc == 3) {
        snprintf(plot_filename, sizeof(plot_filename), "%s_%s.jpg", RedisModule_StringPtrLen(argv[1], len), RedisModule_StringPtrLen(argv[2], len));
        int i=1;
        while (access(plot_filename, F_OK) == 0) {
            printf("FILE EXISTS!\n");
            // file exists
            snprintf(plot_filename, sizeof(plot_filename), "%s_%s%d.jpg", RedisModule_StringPtrLen(argv[1], len), RedisModule_StringPtrLen(argv[2], len), i);
            i++;
        }
    } else if (argc == 5) {
        char *JPG = RedisModule_StringPtrLen(argv[3], len);
        if (strcmp(JPG, "JPG") != 0) {
            return RedisModule_ReplyWithError(ctx, "ERR Invalid syntax! usage: TS.PLOT key function [JPG jpg]");
        }
        snprintf(plot_filename, sizeof(plot_filename), "%s.jpg", RedisModule_StringPtrLen(argv[4], len));
    }

    snprintf(tmp_filename, sizeof(tmp_filename), ".%s.tmp", RedisModule_StringPtrLen(argv[1], len));
    snprintf(command, sizeof(command), "rm -rf %s", tmp_filename);
    system(command);

    Series *series;
	RedisModuleKey *key;
	const int status = GetSeries(ctx, argv[1], &key, &series, REDISMODULE_READ);
	if(!status) {
		return REDISMODULE_ERR;
	}	
	SeriesIterator iterator;
	Sample sample;
	if (SeriesQuery(series, &iterator, 0, series->lastTimestamp, 0, NULL, series->lastTimestamp) != TSDB_OK) {
        return RedisModule_ReplyWithArray(ctx, 0);
    }
	while (SeriesIteratorGetNext(&iterator, &sample) == CR_OK) {
        //ReplyWithSample(ctx, sample.timestamp, sample.value);
        array[arraylen] = sample.value;
        if (array[arraylen] != array[arraylen]) { // NaN
            array[arraylen] = array[arraylen-1];
        }
        arraylen++;
    }
    FILE *fp = fopen(tmp_filename, "w");
    if(fp == NULL){
        printf("File open error!\n");
    }
    for(int i=0; i<arraylen; i++){
        fprintf(fp, "%lf\n", array[i]);
    }
    fclose(fp);
	SeriesIteratorClose(&iterator);

    // function : data, acf, pacf, diff_acf, diff_pacf, smooth, downsampling
    strcpy(function, RedisModule_StringPtrLen(argv[2], len));
    if (!strcmp(function, "data")) {
        snprintf(command, sizeof(command), "python3 ./ts_python/TS.PLOT_data.py %s %s", tmp_filename, plot_filename);
        system(command);
    } else if (!strcmp(function, "acf")) {
        snprintf(command, sizeof(command), "python3 ./ts_python/TS.PLOT_acf.py %s %s", tmp_filename, plot_filename);
        system(command);
    } else if (!strcmp(function, "pacf")) {
        if (arraylen < 100) {
            return RedisModule_ReplyWithError(ctx, "ERR Too few samples! You need at least 100 samples to plot pacf");
        }
        snprintf(command, sizeof(command), "python3 ./ts_python/TS.PLOT_pacf.py %s %s", tmp_filename, plot_filename);
        system(command);
    } else if (!strcmp(function, "smooth")) {
        if (arraylen < 25) {
            return RedisModule_ReplyWithError(ctx, "ERR Too few samples! You need at least 25 samples to plot smooth");
        }
        snprintf(command, sizeof(command), "python3 ./ts_python/TS.PLOT_smooth.py %s %s", tmp_filename, plot_filename);
        system(command);
    } else if (!strcmp(function, "downsampling")) {
        if (arraylen < 50) {
            return RedisModule_ReplyWithError(ctx, "ERR Too few samples! You need at least 50 samples to plot downsampling");
        }
        snprintf(command, sizeof(command), "python3 ./ts_python/TS.PLOT_downsampling.py %s %s", tmp_filename, plot_filename);
        system(command);
    } else if (!strcmp(function, "diff_acf")) {
        if (arraylen < 3) {
            return RedisModule_ReplyWithError(ctx, "ERR Too few samples! You need at least 3 samples to plot diff_acf");
        }
        snprintf(command, sizeof(command), "python3 ./ts_python/TS.PLOT_diff_acf.py %s %s", tmp_filename, plot_filename);
        system(command);
    } else if (!strcmp(function, "diff_pacf")) {
        if (arraylen < 40) {
            return RedisModule_ReplyWithError(ctx, "ERR Too few samples! You need at least 40 samples to plot diff_pacf");
        }
        snprintf(command, sizeof(command), "python3 ./ts_python/TS.PLOT_diff_pacf.py %s %s", tmp_filename, plot_filename);
        system(command);
    } else {
        return RedisModule_ReplyWithError(ctx, "ERR Invalid function! Available functions : data, acf, pacf, diff_acf, diff_pacf, smooth, downsampling");
    }

    ////////////////////////////////
    // ANSI COLOR CODES           //
    ////////////////////////////////
    // Black: \x1B[30m          //
    // Red: \x1B[31m            //
    // Green: \x1B[32m          //
    // Yellow: \x1B[33m         //
    // Blue: \x1B[34m           //
    // Magenta: \x1B[35m        //
    // Cyan: \x1B[36m           //
    // White: \x1B[37m          //
    // Reset: \x1B[0m           //
    ////////////////////////////////

    char printed_tmp_filename[1000000];
    char printed_plot_filename[1000000];
    
    //snprintf(printed_tmp_filename, sizeof(printed_tmp_filename), "\x1B[33m%s\x1B[0m", tmp_filename);
    
    //snprintf(printed_plot_filename, sizeof(printed_plot_filename), "\x1B[31m%s\x1B[0m", plot_filename);
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    
    //ReplyLen = ReplyLen + 3;
    ReplyLen++;
    //RedisModule_ReplyWithSimpleString(ctx, "\x1B[34mHello World\x1B[0m");
    //RedisModule_ReplyWithSimpleString(ctx, printed_tmp_filename);
    //RedisModule_ReplyWithSimpleString(ctx, printed_plot_filename);
    char tmpReplyString[1000000];
    snprintf(tmpReplyString, sizeof(tmpReplyString), "\x1B[33m%s\x1B[0m line chart has created (\x1B[31m%s\x1B[0m)", RedisModule_StringPtrLen(argv[2], len), realpath(plot_filename, NULL));

    RedisModule_ReplyWithSimpleString(ctx, tmpReplyString);

	RedisModule_ReplySetArrayLength(ctx, ReplyLen);

    snprintf(command, sizeof(command), "rm -rf .%s.tmp", RedisModule_StringPtrLen(argv[1], len));
    system(command);

	return REDISMODULE_OK;
}

int TSDB_downsampling(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    RedisModule_AutoMemory(ctx);
    int interval = 0;
    size_t len;
    char key_Name[1000000];
    char newkey[1000000];
    //double array[250000];
	//int arraylen = 0;
    Sample sampleArray[100000];
    int sampleLen = 0;
    int ReplyLen = 0;
    if (argc != 2 && argc != 4 && argc != 6) {
        return RedisModule_WrongArity(ctx);
    } else if (argc == 2) {
        strcpy(key_Name, RedisModule_StringPtrLen(argv[1], len));
        interval = 5;
        snprintf(newkey, sizeof(newkey), "%s_downsampling_%d", key_Name, interval);
    } else if (argc == 4) {
        if (!strcmp(RedisModule_StringPtrLen(argv[2], len), "INTERVAL")) {
            // NOT HANDLE THE CASE WHERE interval IS NOT AN INTEGER
            interval = atoi(RedisModule_StringPtrLen(argv[3], len));
            strcpy(key_Name, RedisModule_StringPtrLen(argv[1], len));
            snprintf(newkey, sizeof(newkey), "%s_downsampling_%d", key_Name, interval);
        } else if (!strcmp(RedisModule_StringPtrLen(argv[2], len), "NEWKEY")) {
            interval = 5;
            snprintf(newkey, sizeof(newkey), "%s", RedisModule_StringPtrLen(argv[3], len));
        } else {
            return RedisModule_ReplyWithError(ctx, "ERR Invalid syntax! usage: TS.DOWNSAMPLING key [INTERVAL interval] [NEWKEY newkey]");
        }
    } else if (argc == 6) {
        if ( ! ( (!strcmp(RedisModule_StringPtrLen(argv[2], len), "INTERVAL") && !strcmp(RedisModule_StringPtrLen(argv[4], len), "NEWKEY")) || (!strcmp(RedisModule_StringPtrLen(argv[4], len), "INTERVAL") && !strcmp(RedisModule_StringPtrLen(argv[2], len), "NEWKEY")) ) ) {
            return RedisModule_ReplyWithError(ctx, "ERR Invalid syntax! usage: TS.DOWNSAMPLING key [INTERVAL interval] [NEWKEY newkey]");
        }
        if (!strcmp(RedisModule_StringPtrLen(argv[2], len), "INTERVAL") && !strcmp(RedisModule_StringPtrLen(argv[4], len), "NEWKEY")) {
            // NOT HANDLE THE CASE WHERE interval IS NOT AN INTEGER
            interval = atoi(RedisModule_StringPtrLen(argv[3], len));
            snprintf(newkey, sizeof(newkey), "%s", RedisModule_StringPtrLen(argv[5], len));
        } else if (!strcmp(RedisModule_StringPtrLen(argv[2], len), "NEWKEY") && !strcmp(RedisModule_StringPtrLen(argv[4], len), "INTERVAL")) {
            // NOT HANDLE THE CASE WHERE interval IS NOT AN INTEGER
            snprintf(newkey, sizeof(newkey), "%s", RedisModule_StringPtrLen(argv[3], len));
            interval = atoi(RedisModule_StringPtrLen(argv[5], len));
        }
    }

    Series *series;
	RedisModuleKey *key;
	const int status = GetSeries(ctx, argv[1], &key, &series, REDISMODULE_READ);
	if(!status) {
		return REDISMODULE_ERR;
	}	
	SeriesIterator iterator;
	Sample sample;
	if (SeriesQuery(series, &iterator, 0, series->lastTimestamp, 0, NULL, series->lastTimestamp) != TSDB_OK) {
        return RedisModule_ReplyWithArray(ctx, 0);
    }
    sampleLen = 0;
	while (SeriesIteratorGetNext(&iterator, &sample) == CR_OK) {
        //ReplyWithSample(ctx, sample.timestamp, sample.value);
        sampleArray[sampleLen].timestamp = sample.timestamp;
        sampleArray[sampleLen].value = sample.value;
        sampleLen++;
    }
	SeriesIteratorClose(&iterator);

    
    // Series *series;
    RedisModuleString *keyName = RedisModule_CreateString(ctx, newkey, strlen(newkey));
    CreateCtx cCtx = { 0 };

    key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);

    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(key);
        return RTS_ReplyGeneralError(ctx, "TSDB: key already exists");
    }

    CreateTsKey(ctx, keyName, &cCtx, &series, &key);
    RedisModule_CloseKey(key);

    RedisModule_Log(ctx, "verbose", "created new series");
    // RedisModule_ReplyWithSimpleString(ctx, "OK");
    // RedisModule_ReplicateVerbatim(ctx);

    RedisModuleString* valueStr;
    RedisModuleString* timestampStr;
    // RedisModuleString** tmpArgv = (RedisModuleString **)malloc(2 * (RedisModuleString *));
    RedisModuleString* tmpArgv[2];
    RedisModuleString* tmpArgv0 = RedisModule_CreateString(ctx, "TS.ADD", strlen("TS.ADD"));
    RedisModuleString* tmpArgv1 = RedisModule_CreateString(ctx, newkey, strlen(newkey));
    tmpArgv[0] = tmpArgv0;
    tmpArgv[1] = tmpArgv1;

    for (int i=0; i<sampleLen; i=i+interval) {
        timestampStr = RedisModule_CreateStringFromLongLong(ctx, (long long)sampleArray[i].timestamp);
        valueStr = RedisModule_CreateStringFromDouble(ctx, sampleArray[i].value);
        add_TSDB_downsampling(ctx, keyName, timestampStr, valueStr, tmpArgv, 2);
    }

    ReplyLen++;
    char tmpReplyString[1000000];
    snprintf(tmpReplyString, sizeof(tmpReplyString), "new key \"\x1B[31m%s\x1B[0m\" created", newkey);
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    RedisModule_ReplyWithSimpleString(ctx, tmpReplyString);
	RedisModule_ReplySetArrayLength(ctx, ReplyLen);


	return REDISMODULE_OK;
}

int TSDB_analysis(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);	
	
    size_t len;
    char *operation;
    char ndiffs[] = "ndiffs\0";
    char pacf[] = "pacf\0";
    char acf[] = "acf\0";
    //operation = RedisModule_StringPtrLen(argv[2], len);
    int opr = -1; // for error
    int array_len = 0;


	/***check argument numbers***/

    if (argc < 3)
        return RedisModule_WrongArity(ctx);
	if (argc == 3) { 
        if(strcmp(RedisModule_StringPtrLen(argv[2], len), "ndiffs") != 0)
            return RedisModule_WrongArity(ctx);
    } else if (argc == 4) {
        if(strcmp(RedisModule_StringPtrLen(argv[2], len), "pacf") != 0 && strcmp(RedisModule_StringPtrLen(argv[2], len), "acf") != 0)
            return RedisModule_WrongArity(ctx);
    }

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    char *str_diff_val;
    int diff_val = -1; //diff value for pacf/acf

    /***check operation***/
    if(strcmp(operation, ndiffs) == 0)
    {
        opr = 0; // 
    }
    else if (strcmp(operation, pacf) == 0)
    {
        opr = 1;
        str_diff_val = RedisModule_StringPtrLen(argv[3], len);
    }
    else if (strcmp(operation, acf) == 0)
    {
        opr = 2;
        str_diff_val = RedisModule_StringPtrLen(argv[3], len);
    }
    else
    {
        RedisModule_ReplyWithSimpleString(ctx, "wrong operation!");
        array_len++;
    }     

    /***Get timeseries from db and write into file***/

    Series *series;
	RedisModuleKey *key;
	const int status = GetSeries(ctx, argv[1], &key, &series, REDISMODULE_READ);
	if(!status) {
		return REDISMODULE_ERR;
	}	

    SeriesIterator iterator;
	Sample sample;
	if (SeriesQuery(series, &iterator, 0, series->lastTimestamp, 0, NULL, series->lastTimestamp) != TSDB_OK) {
        return RedisModule_ReplyWithArray(ctx, 0);
    }

    FILE *fp;
    fp = fopen("./ts_analysis/test2.txt", "w");
    if(fp == NULL) {
        return RTS_ReplyGeneralError(ctx, "TSDB: can't open file");
    }
	fprintf(fp, "value\n");
	while (SeriesIteratorGetNext(&iterator, &sample) == CR_OK) {
        fprintf(fp, "%f\n", sample.value);
    }
    
	SeriesIteratorClose(&iterator);
    fclose(fp);

    /***run ndiffs from python***/
    if(opr == 0){
            char cmd[] = "python3 ./ts_python/ndiffs.py ./ts_analysis/test2.txt";
            system(cmd);

            fp = fopen("./ts_analysis/ndiffs.txt", "r");
            if(fp == NULL) {
                return RTS_ReplyGeneralError(ctx, "TSDB: can't open file");
            }
            char ndiffs_val[1];

	        fscanf(fp, "%s", ndiffs_val);

            char ndiffs_output[12] = "ndiffs =   \0";
            ndiffs_output[strlen(ndiffs_output)-2] = ndiffs_val[0];
    
            RedisModule_ReplyWithSimpleString(ctx, ndiffs_output);

            array_len++;
    }
    else if(opr == 1){
        char *cmd1 = "python3 ./ts_python/pacf.py ./ts_analysis/test2.txt ";
        
        char *cmd2 = malloc(strlen(cmd1) + strlen(str_diff_val) + 1); 
       
        strcpy(cmd2, cmd1);
        strcat(cmd2, str_diff_val);
    
        system(cmd2);

        fp = fopen("./ts_analysis/pacf.txt", "r");
        if(fp == NULL) {
            return RTS_ReplyGeneralError(ctx, "TSDB: can't open file");
        }
        char pacf_val[25];
    
        while(fscanf(fp, "%s", pacf_val) != EOF) {
            RedisModule_ReplyWithSimpleString(ctx, pacf_val);
            array_len++;
        }


    }
    else if(opr == 2){
        char *cmd1 = "python3 ./ts_python/acf.py ./ts_analysis/test2.txt ";
        
        char *cmd2 = malloc(strlen(cmd1) + strlen(str_diff_val) + 1); 

        strcpy(cmd2, cmd1);
        strcat(cmd2, str_diff_val);
    
        system(cmd2);
       
        fp = fopen("./ts_analysis/acf.txt", "r");
        if(fp == NULL) {
            return RTS_ReplyGeneralError(ctx, "TSDB: can't open file");
        }
        char acf_val[25];
        while(fscanf(fp, "%s", acf_val) != EOF) {
            RedisModule_ReplyWithSimpleString(ctx, acf_val);
            array_len++;
        }
    }

    RedisModule_ReplySetArrayLength(ctx, array_len);
	
	return REDISMODULE_OK;
}

int TSDB_load(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if(argc < 3) {
        return RedisModule_WrongArity(ctx);
    }
    struct Time_Series_Element *time_array;
    size_t time_series_size;
    size_t len;
    char *file_name;
    file_name = RedisModule_StringPtrLen(argv[2], len);
    FILE *fp;
    fp = fopen(file_name, "r");
    if(fp == NULL) {
        return RTS_ReplyGeneralError(ctx, "TSDB: can't open file");
    }
    char time_buffer[30], value_buffer[30];
    RedisModuleString *keyName = argv[1];
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    long long replylen = 0;
    while(fscanf(fp, "%s %s", time_buffer, value_buffer) != EOF) {
        replylen += 1;
        RedisModuleString *timestampStr = RedisModule_CreateString(ctx, time_buffer, strlen(time_buffer));
        RedisModuleString *valueStr = RedisModule_CreateString(ctx, value_buffer, strlen(value_buffer));
        add(ctx, keyName, timestampStr, valueStr, NULL, -1);
    }
    fclose(fp);
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplySetArrayLength(ctx, replylen);
    return REDISMODULE_OK;
}

int TSDB_print(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
 	RedisModule_AutoMemory(ctx);
    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }
	Series *series;
	RedisModuleKey *key;
	const int status = GetSeries(ctx, argv[1], &key, &series, REDISMODULE_READ);
	if(!status) {
		return REDISMODULE_ERR;
	}	
	SeriesIterator iterator;
	Sample sample;
	if (SeriesQuery(series, &iterator, 0, series->lastTimestamp, 0, NULL, series->lastTimestamp) != TSDB_OK) {
        return RedisModule_ReplyWithArray(ctx, 0);
    }
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	int arraylen = 0;
	while (SeriesIteratorGetNext(&iterator, &sample) == CR_OK) {
        ReplyWithSample(ctx, sample.timestamp, sample.value);
        arraylen++;
    }
	SeriesIteratorClose(&iterator);
	RedisModule_ReplySetArrayLength(ctx, arraylen);
	return REDISMODULE_OK;
}

int TSDB_predict(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {    
    RedisModule_AutoMemory(ctx);
	if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }
    size_t len;
    CreateArima arima = {0};
    arima.model_file  = RedisModule_CreateStringFromLongLong(ctx,100);
    arima.result_file = RedisModule_CreateStringFromLongLong(ctx,100);

    if (parseArimaArgs(ctx, argv, argc, &arima) != REDISMODULE_OK) {
		return REDISMODULE_ERR;
	}
    
    Series *series;
    RedisModuleKey *key;
    const int status = GetSeries(ctx, argv[1], &key, &series, REDISMODULE_READ);
    if(!status) {
		return REDISMODULE_ERR;
	}
    SeriesIterator iterator;
	Sample sample;
	if (SeriesQuery(series, &iterator, 0, series->lastTimestamp, 0, NULL, series->lastTimestamp) != TSDB_OK) {
        return RedisModule_ReplyWithArray(ctx, 0);
    }
    FILE *fp;
    fp = fopen("python_read.txt", "w");
    if(fp == NULL) {
        return RTS_ReplyGeneralError(ctx, "TSDB: can't open file for writing.");
    }
    char *model_file, *result_file;
    model_file  = RedisModule_StringPtrLen(arima.model_file, len);
    result_file = RedisModule_StringPtrLen(arima.result_file, len);
    if(strcmp(model_file, "100") == 0){
        model_file  = "";
    }
    if(strcmp(result_file, "100") == 0){
        result_file = "";
    }
    fprintf(fp, "%d\n", arima.N);
    fprintf(fp, "%s\n", model_file);
    fprintf(fp, "%s\n", result_file);
    while (SeriesIteratorGetNext(&iterator, &sample) == CR_OK) {
        fprintf(fp, "%f\n", sample.value);
    }
    fclose(fp);
    system("python3 ./ts_python/arima_predict.py > python_result_predict.txt");
    fp = fopen("python_result_predict.txt", "r");
    if(fp == NULL) {
        return RTS_ReplyGeneralError(ctx, "TSDB: can't open file for reading.");
    }
    char resultt[10000];
    fseek(fp, 0L, SEEK_END);
    long numbytes = ftell(fp);
    fseek(fp, 0L, SEEK_SET);
    fread(resultt, sizeof(char), numbytes - 10, fp);
    resultt[numbytes - 10] = '\0';
    fclose(fp);
    RedisModule_ReplyWithSimpleString(ctx, resultt);
	return REDISMODULE_OK;
}

int TSDB_train(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);
	if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }
    CreateArima arima = {0};
    size_t len;
    arima.model_file  = RedisModule_CreateStringFromLongLong(ctx,100);
    arima.result_file = RedisModule_CreateStringFromLongLong(ctx,100);
    // parse 
    if (parseArimaArgs(ctx, argv, argc, &arima) != REDISMODULE_OK) {
		return REDISMODULE_ERR;
	}
    char *model_file, *result_file;
    model_file  = RedisModule_StringPtrLen(arima.model_file, len);
    result_file = RedisModule_StringPtrLen(arima.result_file, len);
    if(strcmp(model_file, "100") == 0){
        model_file  = "";
    }
    if(strcmp(result_file, "100") == 0){
        result_file = "";
    }
    Series *series;
    RedisModuleKey *key;
    const int status = GetSeries(ctx, argv[1], &key, &series, REDISMODULE_READ);
    if(!status) {
		return REDISMODULE_ERR;
	}
    SeriesIterator iterator;
	Sample sample;
	if (SeriesQuery(series, &iterator, 0, series->lastTimestamp, 0, NULL, series->lastTimestamp) != TSDB_OK) {
        return RedisModule_ReplyWithArray(ctx, 0);
    }
    //fopen 
    //RedisModule_ReplyWithArray(ctx, 2);
    FILE *fp;
    fp = fopen("python_read.txt", "w");
    if(fp == NULL) {
        return RTS_ReplyGeneralError(ctx, "TSDB: can't open file for writing.");
    }
	printf("%s\n", result_file);
    printf("%s\n", model_file);
    fprintf(fp, "%d\n", arima.p_start);
    fprintf(fp, "%d\n", arima.p_end);
    fprintf(fp, "%d\n", arima.q_start);
    fprintf(fp, "%d\n", arima.q_end);
    fprintf(fp, "%d\n", arima.d);
    fprintf(fp, "%d\n", arima.seasonal);
    fprintf(fp, "%d\n", arima.N);
    fprintf(fp, "%s\n", model_file);
    fprintf(fp, "%s\n", result_file);

    while (SeriesIteratorGetNext(&iterator, &sample) == CR_OK) {
        fprintf(fp, "%f\n", sample.value);
    }
    fclose(fp);

    system("python3 ./ts_python/arima_train.py > python_result_train.txt");
    fp = fopen("python_result_train.txt", "r");
    if(fp == NULL) {
        return RTS_ReplyGeneralError(ctx, "TSDB: can't open file for reading.");
    }
    char result[5000];
    fseek(fp, 0L, SEEK_END);
    long numbytes = ftell(fp);
    fseek(fp, 0L, SEEK_SET);
    fread(result, sizeof(char), numbytes - 10, fp);
    result[numbytes - 10] = '\0';
    fclose(fp);
    RedisModule_ReplyWithSimpleString(ctx, result);
	
	return REDISMODULE_OK;
}

int TSDB_pacf(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    char keyName[1000000];
    double array[100000];
    char tmp_filename[1000000];
    int arraylen = 0;
    size_t len;
    if (argc < 3){
        return RedisModule_WrongArity(ctx);
    }
    int order_of_difference = atoi(RedisModule_StringPtrLen(argv[2], len));
    if (order_of_difference > 2){
        return RedisModule_ReplyWithError(ctx, "ERR Order of difference cannot be larger than 2");
    }
    strcpy(keyName, RedisModule_StringPtrLen(argv[1], len));
    
    Series *series;
	RedisModuleKey *key;
	const int status = GetSeries(ctx, argv[1], &key, &series, REDISMODULE_READ);
	if(!status) {
		return REDISMODULE_ERR;
	}	
	SeriesIterator iterator;
	Sample sample;
	if (SeriesQuery(series, &iterator, 0, series->lastTimestamp, 0, NULL, series->lastTimestamp) != TSDB_OK) {
        return RedisModule_ReplyWithArray(ctx, 0);
    }
    arraylen = 0;
	while (SeriesIteratorGetNext(&iterator, &sample) == CR_OK) {
        //ReplyWithSample(ctx, sample.timestamp, sample.value);
        array[arraylen] = sample.value;
        if (array[arraylen] != array[arraylen]) { // NaN
            array[arraylen] = array[arraylen-1];
        }
        arraylen++;
    }
	SeriesIteratorClose(&iterator);

    snprintf(tmp_filename, sizeof(tmp_filename), ".%s.tmp", RedisModule_StringPtrLen(argv[1], len));
    FILE *fp = fopen(tmp_filename, "w");
    if(fp == NULL){
        printf("File open error!\n");
    }
    for(int i=0; i<arraylen; i++){
        fprintf(fp, "%lf\n", array[i]);
    }
    fclose(fp);

    char command[100000];
    snprintf(command, sizeof(command), "python3 ./ts_python/TS.PACF.py %s %d", tmp_filename, order_of_difference);
    system(command);

    double pacf[100000];
    int pacfLen = 0;

    fp = fopen(tmp_filename, "r");
    while(fscanf(fp, "%lf", &pacf[pacfLen]) != EOF) {
        if(pacf[pacfLen] != pacf[pacfLen]){     // There are some fucking NaNs in the datafile
            pacf[pacfLen] = pacf[pacfLen-1];    // impute with the previous value
        }
        pacfLen++;
    }

    for(int i=0; i<=pacfLen; i++){
        printf("pacf(t = %d) = %lf\n", i, pacf[i]);
    }
    int plot_pacf_length = pacfLen;
    char plot_pacf_arr[21][plot_pacf_length];
    for (int i=0; i<11; i++)
        for (int j=0; j<plot_pacf_length; j++)
            plot_pacf_arr[i][j] = (pacf[j] >= (10-i)*0.1) ? '*' : ' ';


    for (int i=11; i<21; i++)
        for (int j=0; j<plot_pacf_length; j++)
            plot_pacf_arr[i][j] = (pacf[j] <= (10-i)*0.1) ? '*' : ' ';
    for(int i=0; i<plot_pacf_length; i++)
        plot_pacf_arr[10][i] = '*';
    
    for (int i=0; i<21; i++) {
        for (int j=0; j<plot_pacf_length; j++) {
            printf("%c", plot_pacf_arr[i][j]);
        }
        printf("\n");
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////

    int ReplyLen = 21;
    char tmpReplyString[100000];
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    for (int i=0; i<21; i++){
        //memset(tmpReplyString, 0, sizeof(tmpReplyString));
        snprintf(tmpReplyString, sizeof(tmpReplyString), "%+3.2f  ", (double)(10.0-i)*0.1);
        if (i==10) {
            snprintf(tmpReplyString, sizeof(tmpReplyString), " %3.2f  ", 0.0);
        }
        for (int j=0; j<plot_pacf_length; j++){
            if (pacf[j] > 0.1 || pacf[j] < -0.1){
                strcat(tmpReplyString, "\x1B[31m");
                if (plot_pacf_arr[i][j] == '*') {
                    strcat(tmpReplyString, "*");
                } else {
                    strcat(tmpReplyString, " ");
                }
                strcat(tmpReplyString, "\x1B[0m");
            } else {
                strcat(tmpReplyString, "\x1B[33m");
                if (plot_pacf_arr[i][j] == '*') {
                    strcat(tmpReplyString, "*");
                } else {
                    strcat(tmpReplyString, " ");
                }
                strcat(tmpReplyString, "\x1B[0m");
            }
        }
        printf("tmpReplyString: %s\n", tmpReplyString);
        RedisModule_ReplyWithSimpleString(ctx, tmpReplyString);
    }

	RedisModule_ReplySetArrayLength(ctx, ReplyLen);    

    return REDISMODULE_OK;
}

int TSDB_acf(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    char keyName[1000000];
    double array[100000];
    char tmp_filename[1000000];
    int arraylen = 0;
    size_t len;
    if (argc < 3){
        return RedisModule_WrongArity(ctx);
    }
    int order_of_difference = atoi(RedisModule_StringPtrLen(argv[2], len));
    if (order_of_difference > 2){
        return RedisModule_ReplyWithError(ctx, "ERR Order of difference cannot be larger than 2");
    }
    strcpy(keyName, RedisModule_StringPtrLen(argv[1], len));
    
    Series *series;
	RedisModuleKey *key;
	const int status = GetSeries(ctx, argv[1], &key, &series, REDISMODULE_READ);
	if(!status) {
		return REDISMODULE_ERR;
	}	
	SeriesIterator iterator;
	Sample sample;
	if (SeriesQuery(series, &iterator, 0, series->lastTimestamp, 0, NULL, series->lastTimestamp) != TSDB_OK) {
        return RedisModule_ReplyWithArray(ctx, 0);
    }
    arraylen = 0;
	while (SeriesIteratorGetNext(&iterator, &sample) == CR_OK) {
        //ReplyWithSample(ctx, sample.timestamp, sample.value);
        array[arraylen] = sample.value;
        if (array[arraylen] != array[arraylen]) { // NaN
            array[arraylen] = array[arraylen-1];
        }
        arraylen++;
    }
	SeriesIteratorClose(&iterator);

    snprintf(tmp_filename, sizeof(tmp_filename), ".%s.tmp", RedisModule_StringPtrLen(argv[1], len));
    FILE *fp = fopen(tmp_filename, "w");
    if(fp == NULL){
        printf("File open error!\n");
    }
    for(int i=0; i<arraylen; i++){
        fprintf(fp, "%lf\n", array[i]);
    }
    fclose(fp);

    char command[100000];
    snprintf(command, sizeof(command), "python3 ./ts_python/TS.ACF.py %s %d", tmp_filename, order_of_difference);
    system(command);

    double acf[100000];
    int acfLen = 0;

    fp = fopen(tmp_filename, "r");
    while(fscanf(fp, "%lf", &acf[acfLen]) != EOF) {
        if(acf[acfLen] != acf[acfLen]){     // There are some fucking NaNs in the datafile
            acf[acfLen] = acf[acfLen-1];    // impute with the previous value
        }
        acfLen++;
    }

    for(int i=0; i<=acfLen; i++){
        printf("acf(t = %d) = %lf\n", i, acf[i]);
    }
    int plot_acf_length = acfLen;
    char plot_acf_arr[21][plot_acf_length];
    for (int i=0; i<11; i++)
        for (int j=0; j<plot_acf_length; j++)
            plot_acf_arr[i][j] = (acf[j] >= (10-i)*0.1) ? '*' : ' ';


    for (int i=11; i<21; i++)
        for (int j=0; j<plot_acf_length; j++)
            plot_acf_arr[i][j] = (acf[j] <= (10-i)*0.1) ? '*' : ' ';
    for(int i=0; i<plot_acf_length; i++)
        plot_acf_arr[10][i] = '*';
    
    for (int i=0; i<21; i++) {
        for (int j=0; j<plot_acf_length; j++) {
            printf("%c", plot_acf_arr[i][j]);
        }
        printf("\n");
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////

    int ReplyLen = 21;
    char tmpReplyString[100000];
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    for (int i=0; i<21; i++){
        //memset(tmpReplyString, 0, sizeof(tmpReplyString));
        snprintf(tmpReplyString, sizeof(tmpReplyString), "%+3.2f  ", (double)(10.0-i)*0.1);
        if (i==10) {
            snprintf(tmpReplyString, sizeof(tmpReplyString), " %3.2f  ", 0.0);
        }
        for (int j=0; j<plot_acf_length; j++){
            if (acf[j] > 0.1 || acf[j] < -0.1){
                strcat(tmpReplyString, "\x1B[31m");
                if (plot_acf_arr[i][j] == '*') {
                    strcat(tmpReplyString, "*");
                } else {
                    strcat(tmpReplyString, " ");
                }
                strcat(tmpReplyString, "\x1B[0m");
            } else {
                strcat(tmpReplyString, "\x1B[33m");
                if (plot_acf_arr[i][j] == '*') {
                    strcat(tmpReplyString, "*");
                } else {
                    strcat(tmpReplyString, " ");
                }
                strcat(tmpReplyString, "\x1B[0m");
            }
        }
        printf("tmpReplyString: %s\n", tmpReplyString);
        RedisModule_ReplyWithSimpleString(ctx, tmpReplyString);
    }

	RedisModule_ReplySetArrayLength(ctx, ReplyLen);    

    return REDISMODULE_OK;
}

int NotifyCallback(RedisModuleCtx *original_ctx,
                   int type,
                   const char *event,
                   RedisModuleString *key) {
    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModule_AutoMemory(ctx);

    if (strcasecmp(event, "del") == 0) {
        CleanLastDeletedSeries(ctx, key);
    }

    RedisModule_FreeThreadSafeContext(ctx);

    return REDISMODULE_OK;
}

void module_loaded(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
    if (subevent != REDISMODULE_SUBEVENT_MODULE_LOADED) {
        return;
    }
    RedisModuleModuleChange *moduleChange = (RedisModuleModuleChange *)data;
    if (strcasecmp(moduleChange->module_name, "rg") == 0) {
        register_rg(ctx);
    }
}

/*
module loading function, possible arguments:
/*
module loading function, possible arguments:
COMPACTION_POLICY - compaction policy from parse_policies,h
RETENTION_POLICY - long that represents the retention in milliseconds
MAX_SAMPLE_PER_CHUNK - how many samples per chunk
example:
redis-server --loadmodule ./redistimeseries.so COMPACTION_POLICY
"max:1m:1d;min:10s:1h;avg:2h:10d;avg:3d:100d" RETENTION_POLICY 3600 MAX_SAMPLE_PER_CHUNK 1024
*/
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "timeseries", REDISTIMESERIES_MODULE_VERSION, REDISMODULE_APIVER_1) ==
        REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    RedisModule_Log(ctx,
                    "notice",
                    "RedisTimeSeries version %d, git_sha=%s",
                    REDISTIMESERIES_MODULE_VERSION,
                    REDISTIMESERIES_GIT_SHA);

    RTS_GetRedisVersion();
    RedisModule_Log(ctx,
                    "notice",
                    "Redis version found by RedisTimeSeries : %d.%d.%d - %s",
                    RTS_currVersion.redisMajorVersion,
                    RTS_currVersion.redisMinorVersion,
                    RTS_currVersion.redisPatchVersion,
                    RTS_IsEnterprise() ? "enterprise" : "oss");
    if (RTS_IsEnterprise()) {
        RedisModule_Log(ctx,
                        "notice",
                        "Redis Enterprise version found by RedisTimeSeries : %d.%d.%d-%d",
                        RTS_RlecMajorVersion,
                        RTS_RlecMinorVersion,
                        RTS_RlecPatchVersion,
                        RTS_RlecBuild);
    }

    if (RTS_CheckSupportedVestion() != REDISMODULE_OK) {
        RedisModule_Log(ctx,
                        "warning",
                        "Redis version is to old, please upgrade to redis "
                        "%d.%d.%d and above.",
                        RTS_minSupportedVersion.redisMajorVersion,
                        RTS_minSupportedVersion.redisMinorVersion,
                        RTS_minSupportedVersion.redisPatchVersion);
        return REDISMODULE_ERR;
    }

    if (ReadConfig(ctx, argv, argc) == TSDB_ERROR) {
        return REDISMODULE_ERR;
    }

    // ignore errors from redis gears registration, this can fail if the module is not loaded.
    register_rg(ctx);

    RedisModuleTypeMethods tm = { .version = REDISMODULE_TYPE_METHOD_VERSION,
                                  .rdb_load = series_rdb_load,
                                  .rdb_save = series_rdb_save,
                                  .aof_rewrite = RMUtil_DefaultAofRewrite,
                                  .mem_usage = SeriesMemUsage,
                                  .free = FreeSeries };

    SeriesType = RedisModule_CreateDataType(ctx, "TSDB-TYPE", TS_SIZE_RDB_VER, &tm);
    if (SeriesType == NULL)
        return REDISMODULE_ERR;
    IndexInit();
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "ts.create", TSDB_create);
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "ts.alter", TSDB_alter);
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "ts.createrule", TSDB_createRule);
    RMUtil_RegisterWriteCmd(ctx, "ts.deleterule", TSDB_deleteRule);
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "ts.add", TSDB_add);
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "ts.incrby", TSDB_incrby);
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "ts.decrby", TSDB_incrby);
    RMUtil_RegisterReadCmd(ctx, "ts.range", TSDB_range);
    RMUtil_RegisterReadCmd(ctx, "ts.revrange", TSDB_revrange);

    if (RedisModule_CreateCommand(ctx, "ts.queryindex", TSDB_queryindex, "readonly", 0, 0, 0) ==
        REDISMODULE_ERR)
        return REDISMODULE_ERR;

    RMUtil_RegisterReadCmd(ctx, "ts.info", TSDB_info);

    if(RedisModule_CreateCommand(ctx, "ts.load", TSDB_load, "write deny-oom", 1, -1, 3) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    
    if(RedisModule_CreateCommand(ctx, "ts.pacf", TSDB_pacf, "write deny-oom", 1, -1, 3) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    
    if(RedisModule_CreateCommand(ctx, "ts.acf", TSDB_acf, "write deny-oom", 1, -1, 3) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
   	
	if(RedisModule_CreateCommand(ctx, "ts.train", TSDB_train, "write deny-oom", 1, -1, 3) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	if(RedisModule_CreateCommand(ctx, "ts.print", TSDB_print, "write deny-oom", 1, -1, 3) == REDISMODULE_ERR)  
	 	return REDISMODULE_ERR;
	
	if(RedisModule_CreateCommand(ctx, "ts.help", TSDB_help, "write deny-oom", 1, -1, 3) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
	
	if(RedisModule_CreateCommand(ctx, "ts.predict", TSDB_predict, "write deny-oom", 1, -1, 3) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
	
	if(RedisModule_CreateCommand(ctx, "ts.plot", TSDB_plot, "write deny-oom", 1, -1, 3) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

	if(RedisModule_CreateCommand(ctx, "ts.downsampling", TSDB_downsampling, "write deny-oom", 1, -1, 3) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

	if(RedisModule_CreateCommand(ctx, "ts.analysis", TSDB_analysis, "write deny-oom", 1, -1, 3) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "ts.madd", TSDB_madd, "write deny-oom", 1, -1, 3) ==
        REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "ts.mrange", TSDB_mrange, "readonly", 0, 0, 0) ==
        REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "ts.mrevrange", TSDB_mrevrange, "readonly", 0, 0, 0) ==
        REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "ts.mget", TSDB_mget, "readonly", 0, 0, 0) ==
        REDISMODULE_ERR)
        return REDISMODULE_ERR;

    RedisModule_SubscribeToKeyspaceEvents(ctx, REDISMODULE_NOTIFY_GENERIC, NotifyCallback);

    RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_ModuleChange, module_loaded);

    return REDISMODULE_OK;
}
