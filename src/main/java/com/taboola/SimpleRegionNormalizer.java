/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */
package com.taboola;

import java.io.IOException;
import java.time.Instant;
import java.time.Period;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin.MasterSwitchType;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.normalizer.MergeNormalizationPlan;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizer;
import org.apache.hadoop.hbase.master.normalizer.SplitNormalizationPlan;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;


import static org.apache.hadoop.hbase.util.CollectionUtils.isEmpty;

/**
 * Simple implementation of a region normalizer.
 *
 * It is a combination of the SimpleRegionNormalizer from CDH 5.16.2 and HBase commit 2b6a91a1dabcb90b085c522701bfb604a1473e30 (from 2021-05-10.
 *
 * This normalizer can be configured by global settings in {@code hbase-site.xml} or per table using table properties.
 * Not all options can be set in both places.
 *
 * Generic options:
 * <ul>
 *   <li>{@code NORMALIZER_TARGET_REGION_COUNT} (Table setting, Default: -1 = disabled): This setting can be used to steer the normalizer towards a target region count</li>
 *   <li>{@code NORMALIZER_TARGET_REGION_SIZE} (Table setting, Default: -1 = disabled): This setting can be used to steer the normalizer towards an average target region size</li>
 * </ul>
 *
 * Configuration options related to splits:
 * <ul>
 *   <li>{@code hbase.normalizer.split.enabled} (Table & Global setting, Default: true): Controls (optionally per table) whether this normalizer will ever split a region. Note that splits can be temporarily disabled cluster wide as well using a master switch.</li>
 *   <li>{@code hbase.normalizer.split.min_region_size.mb} (Table & Global setting, Default: 0): Regions smaller than this size will not be split by this normalizer</li>
 *   <li>{@code hbase.normalizer.split.min_region_age.days} (Table & Global setting, Default: 3): Only regions older than this will be split</li>
 *   <li>{@code hbase.normalizer.split.multiplier} (Table & Global setting, Default: 2): Regions will be split once they are this many times larger than the average region size</li>
 * </ul>
 *
 * Configuration options related to merges:
 * <ul>
 *   <li>{@code hbase.normalizer.merge.enabled} (Table & Global setting, Default: true): Controls (optionally per table) whether this normalizer will ever merge regions. Note that merges can be temporarily disabled cluster wide as well using a master switch.</li>
 *   <li>{@code hbase.normalizer.merge.min.region.count} (Table & Global setting, Default: 3): Only tables with at least this many regions will be checked for merges</li>
 *   <li>{@code hbase.normalizer.merge.min_region_size.mb} (Table & Global setting, Default: 0): Regions smaller than this size will not be merged by this normalizer</li>
 *   <li>{@code hbase.normalizer.merge.min_region_age.days} (Table & Global setting, Default: 3): Only regions older than this will be merged</li>
 * </ul>
 *
 * Logic in use:
 *
 *  <ol>
 *    <li>System tables are skipped</li>
 *    <li>Splits get a higher priority than merges</li>
 *    <li>If a region is smaller than</li>
 *
 *  <li> get all regions of a given table
 *  <li> get avg size S of each region (by total size of store files reported in RegionLoad)
 *  <li> If biggest region is bigger than S * 2, it is kindly requested to split,
 *    and normalization stops
 *  <li> Otherwise, two smallest region R1 and its smallest neighbor R2 are kindly requested
 *    to merge, if R1 + R1 &lt;  S, and normalization stops
 *  <li> Otherwise, no action is performed
 * </ol>
 * <p>
 * Region sizes are coarse and approximate on the order of megabytes. Additionally,
 * "empty" regions (less than 1MB, with the previous note) are not merged away. This
 * is by design to prevent normalization from undoing the pre-splitting of a table.
 */
@InterfaceAudience.Private
public class SimpleRegionNormalizer implements RegionNormalizer {

  private static final Log LOG = LogFactory.getLog(SimpleRegionNormalizer.class);

  private MasterServices masterServices;
  private NormalizerConfiguration normalizerConfiguration = new NormalizerConfiguration();

  private static final String SPLIT_ENABLED_KEY = "hbase.normalizer.split.enabled";
  private static final String MERGE_ENABLED_KEY = "hbase.normalizer.merge.enabled";

  // This is part of TableDescriptor in HBase 2 and up
  private static final String NORMALIZER_TARGET_REGION_COUNT = "NORMALIZER_TARGET_REGION_COUNT";
  private static final String NORMALIZER_TARGET_REGION_SIZE = "NORMALIZER_TARGET_REGION_SIZE";

  /**
   * Set the master service.
   *
   * @param masterServices inject instance of MasterServices
   */
  @Override
  public void setMasterServices(MasterServices masterServices) {
    this.masterServices = masterServices;

    normalizerConfiguration = new NormalizerConfiguration(masterServices.getConfiguration());
  }

  /**
   * Computes next most "urgent" normalization action on the table.
   * Action may be either a split, or a merge, or no action.
   *
   * @param table table to normalize
   *
   * @return normalization plan to execute
   */
  @Override
  public List<NormalizationPlan> computePlanForTable(TableName table) throws HBaseIOException {
    if (table == null) {
      return Collections.emptyList();
    }

    HTableDescriptor tableDescriptor;
    try {
      tableDescriptor = masterServices.getTableDescriptors().get(table);
    } catch (IOException e) {
      throw new HBaseIOException(e);
    }

    if (tableDescriptor == null) {
      return Collections.emptyList();
    }

    if (table.isSystemTable()) {
      LOG.debug("Normalization of system table [" + table + "] isn't allowed");
      return Collections.emptyList();
    }

    boolean proceedWithSplitPlanning = proceedWithSplitPlanning(tableDescriptor);
    boolean proceedWithMergePlanning = proceedWithMergePlanning(tableDescriptor);

    if (!proceedWithMergePlanning && !proceedWithSplitPlanning) {
      LOG.debug("Both split and merge are disabled. Skipping normalization of table: " + table);
      return Collections.emptyList();
    }

    NormalizeContext ctx = new NormalizeContext(tableDescriptor);
    if (isEmpty(ctx.getTableRegions())) {
      return Collections.emptyList();
    }

    LOG.debug("Computing normalization plan for table: [" + table + "], number of regions: " + ctx.getTableRegions().size());
    List<NormalizationPlan> plans = new ArrayList<>();
    int splitPlansCount = 0;
    if (proceedWithSplitPlanning) {
      List<NormalizationPlan> splitPlans = computeSplitNormalizationPlans(ctx);
      splitPlansCount = splitPlans.size();
      plans.addAll(splitPlans);
    }

    int mergePlansCount = 0;
    if (proceedWithMergePlanning) {
      List<NormalizationPlan> mergePlans = computeMergeNormalizationPlans(ctx);
      mergePlansCount = mergePlans.size();
      plans.addAll(mergePlans);
    }

    LOG.debug("Computed normalization plans for table [" + table + "]."
        + "Total plans: " + plans.size() + ","
        + "split plans: " + splitPlansCount + ", merge plans: " + mergePlansCount);
    return plans;
  }

  private boolean proceedWithSplitPlanning(HTableDescriptor tableDescriptor) {
    String value = tableDescriptor.getValue(SPLIT_ENABLED_KEY);
    return (value == null ? normalizerConfiguration.isSplitEnabled() : Boolean.parseBoolean(value)) &&
        isMasterSwitchEnabled(MasterSwitchType.SPLIT);
  }

  private boolean proceedWithMergePlanning(HTableDescriptor tableDescriptor) {
    String value = tableDescriptor.getValue(MERGE_ENABLED_KEY);
    return (value == null ? normalizerConfiguration.isMergeEnabled() : Boolean.parseBoolean(value)) &&
        isMasterSwitchEnabled(MasterSwitchType.MERGE);
  }

  private boolean isMasterSwitchEnabled(final MasterSwitchType masterSwitchType) {
    // This is not exposed via MasterServices in HBase 1.x so this is potentially dangerous!
    return ((HMaster) masterServices).isSplitOrMergeEnabled(masterSwitchType);
  }

  /**
   * Creates split plans for all regions that are
   * <ul>
   *   <li>open,</li>
   *   <li>old enough (as specified by {@code hbase.normalizer.split.min_region_age.days}),</li>
   *   <li>large enough (as specified by {@code hbase.normalizer.split.min_region_size.mb},</li>
   *   <li>more than {@code hbase.normalizer.split.multiplier} (default: 2) times the size of an average region</li>
   * </ul>
   */
  private List<NormalizationPlan> computeSplitNormalizationPlans(final NormalizeContext ctx) {
    double avgRegionSize = ctx.getAverageRegionSizeMb();
    LOG.debug("Table [" + ctx.getTableName() + "], average region size: " + String.format("%.3f", avgRegionSize) + " MB");

    List<NormalizationPlan> plans = new ArrayList<>();
    for (final HRegionInfo hri : ctx.getTableRegions()) {
      if (skipForSplit(normalizerConfiguration, ctx, hri)) {
        continue;
      }

      long regionSizeMb = getRegionSizeMB(hri);
      if (regionSizeMb > (normalizerConfiguration.getSplitSizeMultiplier(ctx) * avgRegionSize)) {
        LOG.info("Table [" + ctx.getTableName() + "], large region [" + hri.getRegionNameAsString() + "] has size " + regionSizeMb + " MB,"
            + " more than " + normalizerConfiguration.getSplitSizeMultiplier(ctx) + " avg size " + String.format("%.3f", avgRegionSize) + " MB, splitting");
        plans.add(new SplitNormalizationPlan(hri, null));
      }
    }
    return plans;
  }

  /**
   * Determine if a region should be considered for a split operation.
   */
  @SuppressWarnings("TestOnlyProblems") // The required methods are only exposed in HBase 2 and up
  private boolean skipForSplit(
      NormalizerConfiguration normalizerConfiguration,
      NormalizeContext ctx,
      HRegionInfo regionInfo
  ) {
    RegionStates states = ctx.getRegionStates();
    String name = regionInfo.getEncodedName();
    return
        logTraceReason(() -> !states.isRegionInRegionStates(regionInfo), "skipping split of region [" + name + "] because no state information is available.")
            || logTraceReason(() -> !states.isRegionInState(regionInfo, RegionState.State.OPEN), "skipping split of region [" + name + "] because it is not open.")
            || logTraceReason(() -> !isOldEnough(regionInfo, normalizerConfiguration.getSplitMinRegionAge(ctx)), "skipping split of region [" + name + "] because it is not old enough.")
            || logTraceReason(() -> !isLargeEnoughForSplit(normalizerConfiguration, ctx, regionInfo), "skipping split region [" + name + "] because it is not large enough.");
  }

  /**
   * Computes the merge plans that should be executed for this table to converge average region
   * towards target average or target region count.
   */
  private List<NormalizationPlan> computeMergeNormalizationPlans(NormalizeContext ctx) {
    List<HRegionInfo> tableRegions = ctx.getTableRegions();
    if (tableRegions.size() < normalizerConfiguration.getMergeMinRegionCount(ctx)) {
      LOG.debug("Table " + ctx.getTableName() + " has " + tableRegions.size() + " regions,"
          + " required min number of regions for normalizer to run is " + normalizerConfiguration.getMergeMinRegionCount(ctx) + ", not computing merge plans.");
      return Collections.emptyList();
    }

    long avgRegionSizeMb = (long) ctx.getAverageRegionSizeMb();
    // TODO: Does this make sense?
    if (avgRegionSizeMb < normalizerConfiguration.getMergeMinRegionSizeMb(ctx)) {
      return Collections.emptyList();
    }
    LOG.debug("Computing normalization plan for table " + ctx.getTableName() + ". average region size: " + avgRegionSizeMb + " MB,"
        + " number of regions: " + tableRegions.size());

    // this nested loop walks the table's region chain once, looking for contiguous sequences of
    // regions that meet the criteria for merge. The outer loop tracks the starting point of the
    // next sequence, the inner loop looks for the end of that sequence. A single sequence becomes
    // an instance of MergeNormalizationPlan.
    List<NormalizationPlan> plans = new LinkedList<>();
    int candidateIdx = 0;
    while (candidateIdx < tableRegions.size()) {
      if (candidateIdx == tableRegions.size() - 1) {
        break;
      }
      HRegionInfo candidateRegion1 = tableRegions.get(candidateIdx);
      if (skipForMerge(normalizerConfiguration, ctx, candidateRegion1)) {
        candidateIdx++;
        continue;
      }

      HRegionInfo candidateRegion2 = tableRegions.get(candidateIdx + 1);
      if (skipForMerge(normalizerConfiguration, ctx, candidateRegion2)) {
        candidateIdx += 2;
        continue;
      }

      long candidateRegion1Size = getRegionSizeMB(candidateRegion1);
      long candidateRegion2Size = getRegionSizeMB(candidateRegion2);

      if (candidateRegion1Size + candidateRegion2Size < avgRegionSizeMb) {
        LOG.info("Table [" + ctx.getTableName() + "], region [" + candidateRegion1.getEncodedName() + "] (size: " + candidateRegion1Size + ")"
            + "plus neighbor region [" + candidateRegion2.getEncodedName() + "] (size: " + candidateRegion2Size + ")"
            + "are smaller than the average region size (" + avgRegionSizeMb + "), merging them");
        plans.add(new MergeNormalizationPlan(candidateRegion1, candidateRegion2));
        candidateIdx++; // Skips the next one because it's already part of the current plan
      }

      candidateIdx++;
    }

    return plans;
  }

  /**
   * Determine if a {@link RegionInfo} should be considered for a merge operation.
   * </p>
   * Callers beware: for safe concurrency, be sure to pass in the local instance of
   * {@link NormalizerConfiguration}, don't use {@code this}'s instance.
   */
  @SuppressWarnings("TestOnlyProblems") // The required methods are only exposed in HBase 2 and up
  private boolean skipForMerge(
      final NormalizerConfiguration normalizerConfiguration,
      final NormalizeContext ctx,
      final HRegionInfo regionInfo
  ) {
    RegionStates states = ctx.getRegionStates();
    String name = regionInfo.getEncodedName();
    return
        logTraceReason(() -> !states.isRegionInRegionStates(regionInfo), "skipping merge of region [" + name + "] because no state information is available.")
            || logTraceReason(() -> !states.isRegionInState(regionInfo, State.OPEN), "skipping merge of region [" + name + "] because it is not open.")
            || logTraceReason(() -> !isOldEnough(regionInfo, normalizerConfiguration.getMergeMinRegionAge(ctx)), "skipping merge of region [" + name + "] because it is not old enough.")
            || logTraceReason(() -> !isLargeEnoughForMerge(normalizerConfiguration, ctx, regionInfo), "skipping merge region [" + name + "] because it is not large enough.");
  }

  /**
   * Return {@code true} when {@code regionInfo} has a size that is sufficient
   * to be considered for a merge operation, {@code false} otherwise.
   * </p>
   * Callers beware: for safe concurrency, be sure to pass in the local instance of
   * {@link NormalizerConfiguration}, don't use {@code this}'s instance.
   */
  private boolean isLargeEnoughForMerge(
      final NormalizerConfiguration normalizerConfiguration,
      final NormalizeContext ctx,
      final HRegionInfo regionInfo
  ) {
    return getRegionSizeMB(regionInfo) >= normalizerConfiguration.getMergeMinRegionSizeMb(ctx);
  }

  /**
   * Return {@code true} when {@code regionInfo} has a size that is sufficient
   * to be considered for a split operation, {@code false} otherwise.
   * </p>
   * Callers beware: for safe concurrency, be sure to pass in the local instance of
   * {@link NormalizerConfiguration}, don't use {@code this}'s instance.
   */
  private boolean isLargeEnoughForSplit(
      NormalizerConfiguration normalizerConfiguration,
      NormalizeContext ctx,
      HRegionInfo regionInfo
  ) {
    return getRegionSizeMB(regionInfo) >= normalizerConfiguration.getSplitMinRegionSizeMb(ctx);
  }

  /**
   * Return {@code true} when {@code regionInfo} has a creation date that is old
   * enough to be considered for a split operation, {@code false} otherwise.
   */
  private static boolean isOldEnough(
      final HRegionInfo regionInfo,
      final Period minAge
  ) {
    Instant currentTime = Instant.ofEpochMilli(EnvironmentEdgeManager.currentTime());
    Instant regionCreateTime = Instant.ofEpochMilli(regionInfo.getRegionId());
    return currentTime.isAfter(regionCreateTime.plus(minAge));
  }

  /**
   * @return size of region in MB and if region is not found then -1
   */
  private long getRegionSizeMB(HRegionInfo hri) {
    ServerName sn = masterServices.getAssignmentManager().getRegionStates().getRegionServerOfRegion(hri);
    if (sn == null) {
      LOG.debug("[" + hri.getRegionNameAsString() + "] region was not found on any Server");
      return -1;
    }

    ServerLoad serverLoad = masterServices.getServerManager().getLoad(sn);
    if (serverLoad == null) {
      LOG.debug("server [" + sn.getServerName() + "] was not found in ServerManager");
      return -1;
    }

    RegionLoad regionLoad = serverLoad.getRegionsLoad().get(hri.getRegionName());
    if (regionLoad == null) {
      LOG.debug("[" + hri.getRegionNameAsString() + "] was not found in RegionsLoad");
      return -1;
    }

    return regionLoad.getStorefileSizeMB();
  }


  private static boolean logTraceReason(final BooleanSupplier predicate, final String fmtWhenTrue) {
    boolean value = predicate.getAsBoolean();
    if (value) {
      LOG.trace(fmtWhenTrue);
    }
    return value;
  }


  /**
   * Holds the configuration values read from {@link Configuration}. Encapsulation in a POJO
   * enables atomic hot-reloading of configs without locks.
   */
  private static final class NormalizerConfiguration {

    // SPLIT related options
    private static final boolean DEFAULT_SPLIT_ENABLED = true;

    private static final String SPLIT_MIN_REGION_AGE_DAYS_KEY = "hbase.normalizer.split.min_region_age.days";
    private static final int DEFAULT_SPLIT_MIN_REGION_AGE_DAYS = 3;

    private static final String SPLIT_MIN_REGION_SIZE_MB_KEY = "hbase.normalizer.split.min_region_size.mb";
    private static final int DEFAULT_SPLIT_MIN_REGION_SIZE_MB = 0;

    private static final String SPLIT_SIZE_MULTIPLIER_KEY = "hbase.normalizer.split.multiplier";
    private static final double DEFAULT_SPLIT_SIZE_MULTIPLIER = 2;


    // MERGE related options
    private static final boolean DEFAULT_MERGE_ENABLED = true;

    private static final String MERGE_MIN_REGION_COUNT_KEY = "hbase.normalizer.merge.min.region.count";
    private static final int DEFAULT_MERGE_MIN_REGION_COUNT = 3;

    private static final String MERGE_MIN_REGION_AGE_DAYS_KEY = "hbase.normalizer.merge.min_region_age.days";
    private static final int DEFAULT_MERGE_MIN_REGION_AGE_DAYS = 3;

    private static final String MERGE_MIN_REGION_SIZE_MB_KEY = "hbase.normalizer.merge.min_region_size.mb";
    private static final int DEFAULT_MERGE_MIN_REGION_SIZE_MB = 0;


    private final boolean mergeEnabled;
    private final int mergeMinRegionCount;
    private final Period mergeMinRegionAge;
    private final long mergeMinRegionSizeMb;

    private final boolean splitEnabled;
    private final Period splitMinRegionAge;
    private final long splitMinRegionSizeMb;
    private final double splitSizeMultiplier;

    private NormalizerConfiguration() {
      splitEnabled = DEFAULT_SPLIT_ENABLED;
      splitMinRegionAge = Period.ofDays(DEFAULT_SPLIT_MIN_REGION_AGE_DAYS);
      splitMinRegionSizeMb = DEFAULT_SPLIT_MIN_REGION_SIZE_MB;
      splitSizeMultiplier = DEFAULT_SPLIT_SIZE_MULTIPLIER;

      mergeEnabled = DEFAULT_MERGE_ENABLED;
      mergeMinRegionCount = DEFAULT_MERGE_MIN_REGION_COUNT;
      mergeMinRegionAge = Period.ofDays(DEFAULT_MERGE_MIN_REGION_AGE_DAYS);
      mergeMinRegionSizeMb = DEFAULT_MERGE_MIN_REGION_SIZE_MB;
    }

    private NormalizerConfiguration(Configuration conf) {
      splitEnabled = conf.getBoolean(SPLIT_ENABLED_KEY, DEFAULT_SPLIT_ENABLED);
      splitMinRegionAge = parsePeriod(conf, SPLIT_MIN_REGION_AGE_DAYS_KEY, DEFAULT_SPLIT_MIN_REGION_AGE_DAYS);
      splitMinRegionSizeMb = parseLong(conf, SPLIT_MIN_REGION_SIZE_MB_KEY, DEFAULT_SPLIT_MIN_REGION_SIZE_MB);
      splitSizeMultiplier = parseDouble(conf, SPLIT_SIZE_MULTIPLIER_KEY, DEFAULT_SPLIT_SIZE_MULTIPLIER);

      mergeEnabled = conf.getBoolean(MERGE_ENABLED_KEY, DEFAULT_MERGE_ENABLED);
      mergeMinRegionCount = parseMinRegionCount(conf);
      mergeMinRegionAge = parsePeriod(conf, MERGE_MIN_REGION_AGE_DAYS_KEY, DEFAULT_MERGE_MIN_REGION_AGE_DAYS);
      mergeMinRegionSizeMb = parseLong(conf, MERGE_MIN_REGION_SIZE_MB_KEY, DEFAULT_MERGE_MIN_REGION_SIZE_MB);
    }

    private boolean isSplitEnabled() {
      return splitEnabled;
    }

    private long getSplitMinRegionSizeMb(NormalizeContext context) {
      return getLong(context, SPLIT_MIN_REGION_SIZE_MB_KEY, splitMinRegionSizeMb);
    }

    private double getSplitSizeMultiplier(NormalizeContext context) {
      double splitSizeMultiplier = context.getOrDefault(SPLIT_SIZE_MULTIPLIER_KEY, Double::parseDouble, -1.0d);
      if (splitSizeMultiplier < 0) {
        splitSizeMultiplier = this.splitSizeMultiplier;
      }
      return splitSizeMultiplier;
    }

    private Period getSplitMinRegionAge(NormalizeContext context) {
      return getPeriod(context, SPLIT_MIN_REGION_AGE_DAYS_KEY, splitMinRegionAge);
    }


    private boolean isMergeEnabled() {
      return mergeEnabled;
    }

    private int getMergeMinRegionCount(NormalizeContext context) {
      int minRegionCount = context.getOrDefault(MERGE_MIN_REGION_COUNT_KEY, Integer::parseInt, 0);
      if (minRegionCount <= 0) {
        minRegionCount = mergeMinRegionCount;
      }
      return minRegionCount;
    }

    private Period getMergeMinRegionAge(NormalizeContext context) {
      return getPeriod(context, MERGE_MIN_REGION_AGE_DAYS_KEY, mergeMinRegionAge);
    }

    private long getMergeMinRegionSizeMb(NormalizeContext context) {
      return getLong(context, MERGE_MIN_REGION_SIZE_MB_KEY, DEFAULT_MERGE_MIN_REGION_SIZE_MB);
    }


    private long getLong(NormalizeContext context, String key, long defaultValue) {
      long value = context.getOrDefault(key, Long::parseLong, -1L);
      if (value < 0) {
        return defaultValue;
      }
      return value;

    }

    private Period getPeriod(NormalizeContext context, String key, Period defaultValue) {
      int value = context.getOrDefault(key, Integer::parseInt, -1);
      if (value < 0) {
        return defaultValue;
      }
      return Period.ofDays(value);
    }

    private static int parseMinRegionCount(Configuration conf) {
      int parsedValue = conf.getInt(MERGE_MIN_REGION_COUNT_KEY, DEFAULT_MERGE_MIN_REGION_COUNT);
      int settledValue = Math.max(1, parsedValue);
      if (parsedValue != settledValue) {
        warnInvalidValue(MERGE_MIN_REGION_COUNT_KEY, parsedValue, settledValue);
      }
      return settledValue;
    }

    private static Period parsePeriod(Configuration conf, String configKey, int defaultValue) {
      int parsedValue = conf.getInt(configKey, defaultValue);
      int settledValue = Math.max(0, parsedValue);
      if (parsedValue != settledValue) {
        warnInvalidValue(configKey, parsedValue, settledValue);
      }
      return Period.ofDays(settledValue);
    }

    private static long parseLong(Configuration conf, String configKey, long defaultValue) {
      long parsedValue = conf.getLong(configKey, defaultValue);
      long settledValue = Math.max(0, parsedValue);
      if (parsedValue != settledValue) {
        warnInvalidValue(configKey, parsedValue, settledValue);
      }
      return settledValue;
    }

    private static double parseDouble(Configuration conf, String configKey, double defaultValue) {
      double parsedValue = conf.getDouble(configKey, defaultValue);
      return Math.max(0, parsedValue);
    }


    private static <T> void warnInvalidValue(final String key, final T parsedValue, final T settledValue) {
      LOG.warn("Configured value [" + parsedValue + "] for key [" + key + "] is invalid. Setting value to [" + settledValue + "].");
    }

  }

  /**
   * Inner class caries the state necessary to perform a single invocation of
   * {@link #computePlanForTable(TableName)}. Grabbing this data from the assignment manager
   * up-front allows any computed values to be realized just once.
   */
  private final class NormalizeContext {

    private final TableName tableName;
    private final RegionStates regionStates;
    private final List<HRegionInfo> tableRegions;
    private final double averageRegionSizeMb;
    private final HTableDescriptor tableDescriptor;

    private NormalizeContext(final HTableDescriptor tableDescriptor) {
      this.tableDescriptor = tableDescriptor;
      tableName = tableDescriptor.getTableName();
      regionStates = masterServices
          .getAssignmentManager()
          .getRegionStates();
      tableRegions = regionStates.getRegionsOfTable(tableName);
      // The list of regionInfo from getRegionsOfTable() is ordered by regionName.
      // regionName does not necessary guarantee the order by STARTKEY (let's say 'aa1', 'aa1!',
      // in order by regionName, it will be 'aa1!' followed by 'aa1').
      // This could result in normalizer merging non-adjacent regions into one and creates overlaps.
      // In order to avoid that, sort the list by RegionInfo.COMPARATOR.
      // See HBASE-24376
      tableRegions.sort(COMPARATOR); // TODO
      averageRegionSizeMb = calculateAverageRegionSizeMb(tableRegions, this.tableDescriptor);
    }

    private TableName getTableName() {
      return tableName;
    }

    private RegionStates getRegionStates() {
      return regionStates;
    }

    private List<HRegionInfo> getTableRegions() {
      return tableRegions;
    }

    private double getAverageRegionSizeMb() {
      return averageRegionSizeMb;
    }

    /**
     * Calculates the average region size from a list of regions.
     *
     * The behavior can be configured by using two table level properties:
     * <ul>
     *   <li>NORMALIZER_TARGET_REGION_SIZE: If this is larger than 0 it will be returned as the average region size</li>
     *   <li>NORMALIZER_TARGET_REGION_COUNT: See below</li>
     * </ul>
     *
     * If no target count is set it'll calculate the total region size in megabytes and will either divide this by the number of regions
     * or by the target region count (if the setting is larger than 0).
     */
    private double calculateAverageRegionSizeMb(final List<HRegionInfo> tableRegions, final HTableDescriptor tableDescriptor) {
      if (isEmpty(tableRegions)) {
        throw new IllegalStateException("Cannot calculate average size of a table without any regions.");
      }

      TableName table = tableDescriptor.getTableName();
      int targetRegionCount = getOrDefault(tableDescriptor.getValue(NORMALIZER_TARGET_REGION_COUNT), Integer::parseInt, -1);
      long targetRegionSize = getOrDefault(tableDescriptor.getValue(NORMALIZER_TARGET_REGION_SIZE), Integer::parseInt, -1);
      LOG.debug("Table [" + table + "] configured with target region count " + targetRegionCount + ", target region size " + targetRegionSize + " MB");

      double avgRegionSize;
      if (targetRegionSize > 0) {
        avgRegionSize = targetRegionSize;
      } else {
        int regionCount = tableRegions.size();
        long totalSizeMb = tableRegions.stream()
            .mapToLong(SimpleRegionNormalizer.this::getRegionSizeMB)
            .sum();
        if (targetRegionCount > 0) {
          avgRegionSize = totalSizeMb / (double) targetRegionCount;
        } else {
          avgRegionSize = totalSizeMb / (double) regionCount;
        }
        LOG.debug("Table [" + table + "], total aggregated regions size: " + totalSizeMb + " MB and average region size " + String.format("%.3f", avgRegionSize) + " MB");
      }

      return avgRegionSize;
    }


    public <T> T getOrDefault(String key, Function<String, T> function, T defaultValue) {
      String value = tableDescriptor.getValue(key);
      if (value == null) {
        return defaultValue;
      } else {
        return function.apply(value);
      }
    }
  }

  private static final Comparator<HRegionInfo> COMPARATOR = (HRegionInfo lhs, HRegionInfo rhs) -> {
    if (rhs == null) {
      return 1;
    }

    // Are regions of same table?
    int result = lhs.getTable().compareTo(rhs.getTable());
    if (result != 0) {
      return result;
    }

    // Compare start keys.
    result = Bytes.compareTo(lhs.getStartKey(), rhs.getStartKey());
    if (result != 0) {
      return result;
    }

    // Compare end keys.
    result = Bytes.compareTo(lhs.getEndKey(), rhs.getEndKey());

    if (result != 0) {
      if (lhs.getStartKey().length != 0
          && lhs.getEndKey().length == 0) {
        return 1; // this is last region
      }
      if (rhs.getStartKey().length != 0
          && rhs.getEndKey().length == 0) {
        return -1; // o is the last region
      }
      return result;
    }

    // regionId is usually milli timestamp -- this defines older stamps
    // to be "smaller" than newer stamps in sort order.
    if (lhs.getRegionId() > rhs.getRegionId()) {
      return 1;
    }
    if (lhs.getRegionId() < rhs.getRegionId()) {
      return -1;
    }

    int replicaDiff = lhs.getReplicaId() - rhs.getReplicaId();
    if (replicaDiff != 0) {
      return replicaDiff;
    }

    if (lhs.isOffline() == rhs.isOffline()) {
      return 0;
    }
    if (lhs.isOffline()) {
      return -1;
    }

    return 1;
  };

}
