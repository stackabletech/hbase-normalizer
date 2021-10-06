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
import java.nio.charset.StandardCharsets;
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
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Admin.MasterSwitchType;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.normalizer.MergeNormalizationPlan;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizer;
import org.apache.hadoop.hbase.master.normalizer.SplitNormalizationPlan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;


import static org.apache.hadoop.hbase.util.CollectionUtils.isEmpty;

/**
 * Simple implementation of a region normalizer.
 *
 * It is a combination of the SimpleRegionNormalizer from CDH 5.16.2 and HBase commit 21aa553bc1d86f4d0d1dc7f47fcdcee49555e538 (from 2021-05-21).
 *
 * This normalizer can be configured by global settings in {@code hbase-site.xml} or per table using table properties.
 * Not all options can be set in both places.
 *
 * Generic options:
 * <ul>
 *   <li>{@code NORMALIZER_TARGET_REGION_COUNT} (Table setting, Default: -1 = disabled): This setting can be used to steer the normalizer towards a target region count</li>
 *   <li>{@code NORMALIZER_TARGET_REGION_SIZE} (Table setting, Default: -1 = disabled): This setting can be used to steer the normalizer towards an average target region size (in MB)</li>
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
 *         TODO!!!
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
 * "empty" regions (less than 1 MB, with the previous note) are not merged away. This
 * is by design to prevent normalization from undoing the pre-splitting of a table.
 */
@InterfaceAudience.Private
public class SimpleRegionNormalizer implements RegionNormalizer {

  private static final Log LOG = LogFactory.getLog(SimpleRegionNormalizer.class);

  private MasterServices masterServices;
  private NormalizerConfiguration normalizerConfiguration = new NormalizerConfiguration();

  private static final TableName PHOENIX_DEFAULT_CATALOG = TableName.valueOf("SYSTEM.CATALOG");
  private static final TableName PHOENIX_NAMESPACED_CATALOG = TableName.valueOf("SYSTEM", "CATALOG");
  private static final byte[] PHOENIX_CF_BYTES = "0".getBytes(StandardCharsets.UTF_8);
  private static final byte[] PHOENIX_SALT_BUCKETS_BYTES = "SALT_BUCKETS".getBytes(StandardCharsets.UTF_8);

  private static final String SPLIT_ENABLED_KEY = "hbase.normalizer.split.enabled";
  private static final String MERGE_ENABLED_KEY = "hbase.normalizer.merge.enabled";

  // This is part of TableDescriptor in HBase 2 and up
  private static final String NORMALIZER_TARGET_REGION_COUNT = "NORMALIZER_TARGET_REGION_COUNT";
  private static final String NORMALIZER_TARGET_REGION_SIZE = "NORMALIZER_TARGET_REGION_SIZE";

  @Override
  public void setMasterServices(MasterServices masterServices) {
    this.masterServices = masterServices;

    normalizerConfiguration = new NormalizerConfiguration(masterServices.getConfiguration());
  }

  /**
   * Tries to find the HBase table for the Phoenix system catalog.
   * If it can't find the correct one it'll return {@code null}.
   */
  private static Table getPhoenixCatalogTable(Connection connection) throws HBaseIOException {
    try (Admin admin = connection.getAdmin()) {
      if (admin.tableExists(PHOENIX_NAMESPACED_CATALOG)) {
        return connection.getTable(PHOENIX_NAMESPACED_CATALOG);
      }
      if (admin.tableExists(PHOENIX_DEFAULT_CATALOG)) {
        return connection.getTable(PHOENIX_DEFAULT_CATALOG);
      }
      return null;
    } catch (IOException e) {
      throw new HBaseIOException(e);
    }
  }

  /**
   * Will try to find the target table in the Phoenix catalog.
   * It'll first try without a schema and then with a schema matching the namespace of the target table.
   * Will return null if it can't find anything (not an empty result).
   */
  private static Result tryFindTableInPhoenixCatalog(Table table, TableName tableName) throws HBaseIOException {
    String schema = tableName.getNamespaceAsString();
    String phoenixTableName = tableName.getQualifierAsString();

    // This needs to handle the case where a Phoenix table is in the default HBase namespace.
    // It can still be in a different Phoenix schema if it has a name with a dot in it and namespace mapping is disabled.
    if (schema.equals("default")) {
      if (phoenixTableName.contains(".")) {
        String[] split = phoenixTableName.split("\\.");
        if (split.length != 2) {
          LOG.debug("Table name ([" + phoenixTableName + "]) contained more than one dot, this is illegal for a Phoenix table so we should be safe to continue");
          return null;
        }
        schema = split[0];
        phoenixTableName = split[1];
      } else {
        schema = null;
      }
    }

    byte[] phoenixSchemaBytes = null;
    if (schema != null) {
      phoenixSchemaBytes = Bytes.toBytes(schema);
    }

    byte[] phoenixTableBytes = Bytes.toBytes(phoenixTableName);

    // Schema for the key in the catalog table is: <tenant Id OR EMPTY>0<schema name OR EMPTY>0<table name>
    // The schemas "hbase" and "default" can never be created (case-sensitive, e.g. "DEFAULT" is allowed)
    // Every table in the "default" namespace will not have a schema WHEN namespace mapping is enabled, all others should have one.
    // When namespace mapping is disabled, tables can have a schema in the "default" namespace but their name will contain a dot.
    try {
      byte[] key;
      if (schema == null) {
        key = Bytes.add(new byte[] {0, 0}, phoenixTableBytes);
      } else {
        // There is no Bytes.add method with more than three parameters, so we do it manually here
        key = new byte[1 + phoenixSchemaBytes.length + 1 + phoenixTableBytes.length];
        key[0] = 0;
        System.arraycopy(phoenixSchemaBytes, 0, key, 1, phoenixSchemaBytes.length);
        key[1 + phoenixSchemaBytes.length ] = 0;
        System.arraycopy(phoenixTableBytes, 0, key, 1 + phoenixSchemaBytes.length + 1, phoenixTableBytes.length);
      }

      Get get = new Get(key);
      Result result = table.get(get);
      if (result.isEmpty()) {
        LOG.trace("Did NOT find information about the table [" + tableName + "] in the Phoenix catalog");
        return null;
      } else {
        return result;
      }
    } catch (IOException e) {
      throw new HBaseIOException(e);
    }
  }


  // Adapted from Phoenix
  private static int decodeInt(byte[] bytes) throws HBaseIOException {
    if (bytes.length < Bytes.SIZEOF_INT) {
      throw new HBaseIOException("Data for SALT_BUCKETS column does not contain 4 bytes of data as required, it contains " + bytes.length + " bytes instead");
    }
    // Flip sign bit back
    int v = bytes[0] ^ 0x80;
    for (int i = 1; i < Bytes.SIZEOF_INT; i++) {
      v = (v << 8) + (bytes[i] & 0xff);
    }
    return v;
  }

  private boolean skipPhoenixTable(TableName table) throws HBaseIOException {
    ClusterConnection connection = masterServices.getConnection();
    Table phoenixTable = getPhoenixCatalogTable(connection);
    if (phoenixTable == null) {
      LOG.trace("Could not find table for Phoenix catalog, will assume Phoenix is not installed and continue");
      return false;
    }
    LOG.trace("Found Phoenix catalog table: " + phoenixTable);

    Result result = tryFindTableInPhoenixCatalog(phoenixTable, table);
    if (result == null) {
      LOG.debug("Table [" + table + "] does not seem to be a Phoenix table, continuing");
      return false;
    }

    LOG.trace("Table [" + table + "] is a Phoenix table, checking for SALT_BUCKETS");
    byte[] saltBucketBytes = result.getValue(PHOENIX_CF_BYTES, PHOENIX_SALT_BUCKETS_BYTES);
    if (saltBucketBytes != null) {
      int saltBuckets = decodeInt(saltBucketBytes);
      LOG.info("Phoenix table [" + table + "] has " + saltBuckets + " salt buckets, will skip normalizing");
      return true;
    } else {
      LOG.trace("Phoenix table [" + table + "] has no salt buckets, will continue");
      return false;
    }
  }

  /**
   * This is the main entry point for the Normalizer.
   */
  @Override
  public List<NormalizationPlan> computePlanForTable(TableName table) throws HBaseIOException {
    if (masterServices == null) {
      LOG.error("masterServices not initialized, this should not happen, aborting normalizer");
      throw new HBaseIOException("masterServices not initialized");
    }

    if (table == null) {
      LOG.error("table is null, this should not happen");
      return Collections.emptyList();
    }

    HTableDescriptor tableDescriptor;

    try {
      tableDescriptor = masterServices.getTableDescriptors().get(table);
    } catch (IOException e) {
      LOG.error("Caught exception trying to get table descriptor for table [" + table + "]", e);
      throw new HBaseIOException(e);
    }

    if (tableDescriptor == null) {
      LOG.error("Could not retrieve table descriptor for table [" + table + "] aborting");
      return Collections.emptyList();
    }

    LOG.info("Taboola Normalizer running for table [" + table + "]");
    LOG.trace("TableDescriptor for [" + table + "]: " + tableDescriptor);

    if (table.isSystemTable()) {
      LOG.debug("Normalization of system table [" + table + "] isn't allowed");
      return Collections.emptyList();
    }

    /*
    if (normalizerConfiguration.isSkipPhoenixTables()) {
      boolean hasPhoenixCoprocessor = tableDescriptor.getCoprocessors().stream().anyMatch(s -> s.contains("org.apache.phoenix.coprocessor"));
      if (hasPhoenixCoprocessor) {
        LOG.info("Skipping normalizer for table [" + table + "] because it's a Phoenix table. This can be controlled using the property " + NormalizerConfiguration.NORMALIZER_SKIP_PHOENIX_TABLES);
        return Collections.emptyList();

      }
    }
     */

    if (normalizerConfiguration.isSkipPhoenixTables() && skipPhoenixTable(table)) {
      return Collections.emptyList();
    }

    // TODO: Table exists but no SALT_BUCKET

    boolean proceedWithSplitPlanning = proceedWithSplitPlanning(tableDescriptor);
    boolean proceedWithMergePlanning = proceedWithMergePlanning(tableDescriptor);

    if (!proceedWithMergePlanning && !proceedWithSplitPlanning) {
      LOG.info("Both split and merge are disabled. Skipping normalization of table: " + table);
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

    LOG.info("Computed normalization plans for table [" + table + "]. "
        + "Total plans: " + plans.size() + ", "
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
    // This is not exposed via MasterServices in HBase 1.x so this cast is potentially dangerous! (e.g. in tests?)
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
        LOG.debug("Table [" + ctx.getTableName() + "], large region [" + hri.getRegionNameAsString() + "] has size " + regionSizeMb + " MB,"
            + " more than " + normalizerConfiguration.getSplitSizeMultiplier(ctx) + " * avg size " + String.format("%.3f", avgRegionSize) + " MB, splitting");
        plans.add(new SplitNormalizationPlan(hri, null));
      } else {
        LOG.trace("Table [" + ctx.getTableName() + "], region [" + hri.getRegionNameAsString() + "] has size " + regionSizeMb + " MB,"
            + " less than " + normalizerConfiguration.getSplitSizeMultiplier(ctx) + " * avg size " + String.format("%.3f", avgRegionSize) + " MB, NOT splitting");
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
            || logTraceReason(() -> !isLargeEnough(normalizerConfiguration.getSplitMinRegionSizeMb(ctx), regionInfo), "skipping split region [" + name + "] because it is not large enough.");
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

    double originalAverageRegionSizeMb = ctx.getAverageRegionSizeMb();
    double mergeSizeMultiplier = normalizerConfiguration.getMergeSizeMultiplier(ctx);
    long avgRegionSizeMb = (long) (originalAverageRegionSizeMb * mergeSizeMultiplier);
    /*
    // TODO: Does this make sense?
    if (avgRegionSizeMb < normalizerConfiguration.getMergeMinRegionSizeMb(ctx)) {
      return Collections.emptyList();
    }
     */
    LOG.debug("Computing normalization plan for table " + ctx.getTableName() + ". Average region size: " + originalAverageRegionSizeMb + " MB, using multiplier " + mergeSizeMultiplier
        + " means we'll use an region size of " + avgRegionSizeMb + " for calculations, number of regions: " + tableRegions.size());

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

      if ((candidateRegion1Size + candidateRegion2Size < avgRegionSizeMb) || (candidateRegion1Size + candidateRegion2Size < 1)) {
        LOG.debug("Table [" + ctx.getTableName() + "], region [" + candidateRegion1.getEncodedName() + "] (size: " + candidateRegion1Size + ") "
            + "plus neighbor region [" + candidateRegion2.getEncodedName() + "] (size: " + candidateRegion2Size + ") "
            + "are smaller than the average region size (" + avgRegionSizeMb + "), merging them: "
            + (candidateRegion1Size + candidateRegion2Size) + " < " + avgRegionSizeMb);
        plans.add(new MergeNormalizationPlan(candidateRegion1, candidateRegion2));
        candidateIdx++; // Skips the next one because it's already part of the current plan
      } else {
        LOG.trace("Table [" + ctx.getTableName() + "], region [" + candidateRegion1.getEncodedName() + "] (size: " + candidateRegion1Size + ") "
            + "plus neighbor region [" + candidateRegion2.getEncodedName() + "] (size: " + candidateRegion2Size + ") "
            + "are larger than (or equal to) the average region size (" + avgRegionSizeMb + "), NOT merging them: "
            + (candidateRegion1Size + candidateRegion2Size) + " > " + avgRegionSizeMb);
      }

      candidateIdx++;
    }

    return plans;
  }

  /**
   * Determine if a region should be considered for a merge operation.
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
            || logTraceReason(() -> !isLargeEnough(normalizerConfiguration.getMergeMinRegionSizeMb(ctx), regionInfo), "skipping merge region [" + name + "] because it is not large enough.");
  }

  private boolean isLargeEnough(
      long minSize,
      HRegionInfo regionInfo
  ) {
    return getRegionSizeMB(regionInfo) >= minSize;
  }

  /**
   * Return {@code true} when {@code regionInfo} has a creation date that is old
   * enough to be considered for a split or merge operation, {@code false} otherwise.
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
   * Holds the configuration values read from {@link Configuration}.
   * Encapsulation in a POJO enables atomic hot-reloading of configs without locks.
   */
  private static final class NormalizerConfiguration {

    public static final String NORMALIZER_SKIP_PHOENIX_TABLES = "hbase.normalizer.skip.phoenix.tables";
    private static final boolean DEFAULT_NORMALIZER_SKIP_PHOENIX_TABLES = false;

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

    private static final String MERGE_SIZE_MULTIPLIER_KEY = "hbase.normalizer.merge.split.multiplier";
    private static final double DEFAULT_MERGE_SIZE_MULTIPLIER = 1;

    private final boolean skipPhoenixTables;

    private final boolean mergeEnabled;
    private final int mergeMinRegionCount;
    private final Period mergeMinRegionAge;
    private final long mergeMinRegionSizeMb;
    private final double mergeSizeMultiplier;

    private final boolean splitEnabled;
    private final Period splitMinRegionAge;
    private final long splitMinRegionSizeMb;
    private final double splitSizeMultiplier;

    private NormalizerConfiguration() {
      skipPhoenixTables = DEFAULT_NORMALIZER_SKIP_PHOENIX_TABLES;

      splitEnabled = DEFAULT_SPLIT_ENABLED;
      splitMinRegionAge = Period.ofDays(DEFAULT_SPLIT_MIN_REGION_AGE_DAYS);
      splitMinRegionSizeMb = DEFAULT_SPLIT_MIN_REGION_SIZE_MB;
      splitSizeMultiplier = DEFAULT_SPLIT_SIZE_MULTIPLIER;

      mergeEnabled = DEFAULT_MERGE_ENABLED;
      mergeMinRegionCount = DEFAULT_MERGE_MIN_REGION_COUNT;
      mergeMinRegionAge = Period.ofDays(DEFAULT_MERGE_MIN_REGION_AGE_DAYS);
      mergeMinRegionSizeMb = DEFAULT_MERGE_MIN_REGION_SIZE_MB;
      mergeSizeMultiplier = DEFAULT_MERGE_SIZE_MULTIPLIER;
    }

    private NormalizerConfiguration(Configuration conf) {
      skipPhoenixTables = conf.getBoolean(NORMALIZER_SKIP_PHOENIX_TABLES, DEFAULT_NORMALIZER_SKIP_PHOENIX_TABLES);

      splitEnabled = conf.getBoolean(SPLIT_ENABLED_KEY, DEFAULT_SPLIT_ENABLED);
      splitMinRegionAge = parsePeriod(conf, SPLIT_MIN_REGION_AGE_DAYS_KEY, DEFAULT_SPLIT_MIN_REGION_AGE_DAYS);
      splitMinRegionSizeMb = parseLong(conf, SPLIT_MIN_REGION_SIZE_MB_KEY, DEFAULT_SPLIT_MIN_REGION_SIZE_MB);
      splitSizeMultiplier = parseDouble(conf, SPLIT_SIZE_MULTIPLIER_KEY, DEFAULT_SPLIT_SIZE_MULTIPLIER);

      mergeEnabled = conf.getBoolean(MERGE_ENABLED_KEY, DEFAULT_MERGE_ENABLED);
      mergeMinRegionCount = parseMinRegionCount(conf);
      mergeMinRegionAge = parsePeriod(conf, MERGE_MIN_REGION_AGE_DAYS_KEY, DEFAULT_MERGE_MIN_REGION_AGE_DAYS);
      mergeMinRegionSizeMb = parseLong(conf, MERGE_MIN_REGION_SIZE_MB_KEY, DEFAULT_MERGE_MIN_REGION_SIZE_MB);
      mergeSizeMultiplier = parseDouble(conf, MERGE_SIZE_MULTIPLIER_KEY, DEFAULT_MERGE_SIZE_MULTIPLIER);
    }

    private boolean isSkipPhoenixTables() {
      return skipPhoenixTables;
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

    private double getMergeSizeMultiplier(NormalizeContext context) {
      double mergeSizeMultiplier = context.getOrDefault(MERGE_SIZE_MULTIPLIER_KEY, Double::parseDouble, -1.0d);
      if (mergeSizeMultiplier < 0) {
        mergeSizeMultiplier = this.mergeSizeMultiplier;
      }
      return mergeSizeMultiplier;
    }

    private Period getMergeMinRegionAge(NormalizeContext context) {
      return getPeriod(context, MERGE_MIN_REGION_AGE_DAYS_KEY, mergeMinRegionAge);
    }

    private long getMergeMinRegionSizeMb(NormalizeContext context) {
      return getLong(context, MERGE_MIN_REGION_SIZE_MB_KEY, mergeMinRegionSizeMb);
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
      tableRegions.sort(COMPARATOR);
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
      int targetRegionCount = getOrDefault(NORMALIZER_TARGET_REGION_COUNT, Integer::parseInt, -1);
      long targetRegionSize = getOrDefault(NORMALIZER_TARGET_REGION_SIZE, Integer::parseInt, -1);
      LOG.debug("Table [" + table + "] configured with target region count " + targetRegionCount + ", target region size " + targetRegionSize + " MB, if both are set only target region size will take effect");

      int regionCount = tableRegions.size();
      long totalSizeMb = tableRegions.stream()
          .mapToLong(SimpleRegionNormalizer.this::getRegionSizeMB)
          .sum();

      double avgRegionSize = totalSizeMb / (double) regionCount;
      LOG.debug("Table [" + table + "], total aggregated regions size: " + totalSizeMb + " MB and average region size " + String.format("%.3f", avgRegionSize) + " MB");
      if (targetRegionSize > 0) {
        avgRegionSize = targetRegionSize;
        LOG.debug("Table [" + table + "], average region size adjusted to " + String.format("%.3f", avgRegionSize) + " MB to match target region size, will ignore target region count if also set");
      } else {
        if (targetRegionCount > 0) {
          avgRegionSize = totalSizeMb / (double) targetRegionCount;
          LOG.debug("Table [" + table + "], average region size adjusted to " + String.format("%.3f", avgRegionSize) + " MB to match target region count of " + targetRegionCount);
        } else {
          avgRegionSize = totalSizeMb / (double) regionCount;
        }
      }

      return avgRegionSize;
    }


    public <T> T getOrDefault(String key, Function<String, T> function, T defaultValue) {
      String value = tableDescriptor.getValue(key);
      if (value != null) {
        return function.apply(value);
      }

      value = tableDescriptor.getConfigurationValue(key);
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
