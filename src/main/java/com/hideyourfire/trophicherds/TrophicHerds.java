package com.hideyourfire.trophicherds;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.bukkit.Bukkit;
import org.bukkit.HeightMap;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.World;
import org.bukkit.block.Biome;
import org.bukkit.Chunk;
import org.bukkit.GameMode;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.entity.Chicken;
import org.bukkit.entity.Cow;
import org.bukkit.entity.Entity;
import org.bukkit.entity.EntityType;
import org.bukkit.entity.Goat;
import org.bukkit.entity.Horse;
import org.bukkit.entity.Ageable;
import org.bukkit.entity.Mob;
import org.bukkit.entity.Player;
import org.bukkit.entity.Sheep;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.entity.EntityBreedEvent;
import org.bukkit.event.entity.EntityTargetLivingEntityEvent;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;
import org.bukkit.util.Vector;
import com.destroystokyo.paper.event.entity.EntityPathfindEvent;

public final class TrophicHerds extends JavaPlugin {
  private static final List<MobKind<? extends Mob>> SUPPORTED_MOBS = List.of(
      new MobKind<>(EntityType.COW, Cow.class, "cow"),
      new MobKind<>(EntityType.HORSE, Horse.class, "horse"),
      new MobKind<>(EntityType.SHEEP, Sheep.class, "sheep"),
      new MobKind<>(EntityType.CHICKEN, Chicken.class, "chicken"),
      new MobKind<>(EntityType.GOAT, Goat.class, "goat"));

  private Settings settings;
  private boolean debugEnabled;
  private BukkitTask herdTask;
  private final HerdManager herdManager = new HerdManager();
  private static final double HERDCOUNT_RADIUS = 128.0;
  private static final int HERDCOUNT_MIN_POPULATION = 2;

  @Override
  public void onEnable() {
    saveDefaultConfig();
    this.settings = Settings.fromConfig(getConfig());
    this.debugEnabled = getConfig().getBoolean("debug", false);
    getServer().getPluginManager().registerEvents(new PredatorListener(), this);
    scheduleHerdManagement();
  }

  @Override
  public void onDisable() {
    if (herdTask != null) {
      herdTask.cancel();
      herdTask = null;
    }
    saveConfig();
  }

  private void scheduleHerdManagement() {
    herdTask = Bukkit.getScheduler().runTaskTimer(
        this,
        this::tickHerds,
        1L,
        1L);
  }

  private void tickHerds() {
    int currentTick = Bukkit.getCurrentTick();
    for (World world : Bukkit.getWorlds()) {
      for (MobTypeConfig<? extends Mob> config : settings.mobConfigs.values()) {
        if (config.settings.herdUpdateIntervalTicks > 0) {
          herdManager.updateHerdsIfDue(world, config, currentTick, this::logCullingEvent);
        }
      }
    }
  }

  @Override
  public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
    if (!command.getName().equalsIgnoreCase("th")) {
      return false;
    }
    if (args.length == 0) {
      sender.sendMessage("Usage: /th <herdcount|debug>");
      return true;
    }
    if (args[0].equalsIgnoreCase("debug")) {
      return handleDebugCommand(sender, args);
    }
    if (!args[0].equalsIgnoreCase("herdcount")) {
      sender.sendMessage("Usage: /th herdcount");
      return true;
    }
    if (!(sender instanceof Player player)) {
      sender.sendMessage("This command can only be used by players.");
      return true;
    }
    if (!player.hasPermission("trophicherds.admin")) {
      sender.sendMessage("You do not have permission to use this command.");
      return true;
    }

    org.bukkit.block.Block targetBlock = player.getTargetBlockExact(100);
    Location targetLocation = targetBlock != null
        ? targetBlock.getLocation()
        : player.getLocation();
    Biome targetBiome = targetLocation.getBlock().getBiome();

    int currentTick = Bukkit.getCurrentTick();
    for (MobTypeConfig<? extends Mob> config : settings.mobConfigs.values()) {
      if (config.settings.herdUpdateIntervalTicks > 0) {
        herdManager.updateHerdsIfDue(
            player.getWorld(),
            config,
            currentTick,
            this::logCullingEvent);
      }
    }

    List<HerdSnapshot> snapshots = herdManager.getHerdSnapshots(
        player.getWorld(),
        targetBiome,
        targetLocation,
        HERDCOUNT_RADIUS,
        HERDCOUNT_MIN_POPULATION);

    sender.sendMessage(String.format(
        "There are %d herds loaded in this immediate biome with at least %d animals.",
        snapshots.size(),
        HERDCOUNT_MIN_POPULATION));
    for (HerdSnapshot snapshot : snapshots) {
      String typeName = formatEntityType(snapshot.entityType);
      sender.sendMessage(String.format(
          "%s (population: %d)",
          typeName,
          snapshot.population));
      sender.sendMessage(String.format(
          "- Leader is at coords %d, %d, %d",
          snapshot.leaderLocation.getBlockX(),
          snapshot.leaderLocation.getBlockY(),
          snapshot.leaderLocation.getBlockZ()));
    }
    return true;
  }

  private boolean handleDebugCommand(CommandSender sender, String[] args) {
    if (args.length != 2 || !args[0].equalsIgnoreCase("debug")) {
      sender.sendMessage("Usage: /th debug <true|false>");
      return true;
    }
    if (sender instanceof Player player && !player.isOp()) {
      sender.sendMessage("You must be an operator to use this command.");
      return true;
    }
    String value = args[1].trim().toLowerCase(Locale.ROOT);
    if (!value.equals("true") && !value.equals("false")) {
      sender.sendMessage("Usage: /th debug <true|false>");
      return true;
    }
    boolean enabled = Boolean.parseBoolean(value);
    debugEnabled = enabled;
    getConfig().set("debug", enabled);
    sender.sendMessage("TrophicHerds debug is now " + (enabled ? "enabled" : "disabled") + ".");
    return true;
  }

  private void logCullingEvent(String message) {
    getLogger().info(message);
    if (!debugEnabled) {
      return;
    }
    for (Player player : Bukkit.getOnlinePlayers()) {
      if (player.isOp()) {
        player.sendMessage(message);
      }
    }
  }

  private void logDebug(String message) {
    if (!debugEnabled) {
      return;
    }
    getLogger().info(message);
    for (Player player : Bukkit.getOnlinePlayers()) {
      if (player.isOp()) {
        player.sendMessage(message);
      }
    }
  }

  @FunctionalInterface
  private interface DebugSink {
    void log(String message);
  }

  private static final class Settings {
    private final EnumMap<EntityType, MobTypeConfig<? extends Mob>> mobConfigs;
    private final EnumMap<EntityType, List<MobTypeConfig<? extends Mob>>> predatorToPrey;

    private Settings(
        EnumMap<EntityType, MobTypeConfig<? extends Mob>> mobConfigs,
        EnumMap<EntityType, List<MobTypeConfig<? extends Mob>>> predatorToPrey) {
      this.mobConfigs = mobConfigs;
      this.predatorToPrey = predatorToPrey;
    }

    private static Settings fromConfig(FileConfiguration config) {
      EnumMap<EntityType, MobTypeConfig<? extends Mob>> mobConfigs =
          new EnumMap<>(EntityType.class);
      for (MobKind<? extends Mob> kind : SUPPORTED_MOBS) {
        String basePath = "mobs." + kind.configKey;
        if (!config.isConfigurationSection(basePath)) {
          continue;
        }
        MobSettings mobSettings = MobSettings.fromConfig(config, basePath);
        mobConfigs.put(
            kind.entityType,
            new MobTypeConfig<>(kind.entityType, kind.entityClass, mobSettings));
      }
      EnumMap<EntityType, List<MobTypeConfig<? extends Mob>>> predatorToPrey =
          buildPredatorIndex(mobConfigs);
      return new Settings(mobConfigs, predatorToPrey);
    }

    private static EnumMap<EntityType, List<MobTypeConfig<? extends Mob>>> buildPredatorIndex(
        EnumMap<EntityType, MobTypeConfig<? extends Mob>> mobConfigs) {
      EnumMap<EntityType, List<MobTypeConfig<? extends Mob>>> index =
          new EnumMap<>(EntityType.class);
      for (MobTypeConfig<? extends Mob> config : mobConfigs.values()) {
        for (EntityType predator : config.settings.predators) {
          index.computeIfAbsent(predator, ignored -> new ArrayList<>()).add(config);
        }
      }
      return index;
    }
  }

  private static final class MobSettings {
    private final double awarenessDistance;
    private final double herdRadius;
    private final int herdUpdateIntervalTicks;
    private final double fleeSpeed;
    private final double panicSpeed;
    private final double fleeRange;
    private final double panicRange;
    private final java.util.EnumSet<EntityType> predators;
    private final HerdType herdType;
    private final double nightHerdRadiusMultiplier;
    private final java.util.EnumSet<Material> grazeCrops;
    private final int grazeFrequencyTicks;
    private final int softCapPerBiome;
    private final int softCapPerChunk;
    private final int overcapIntervalTicks;
    private final int overcapRemovalsPerInterval;
    private final int minHerdSize;
    private final int densityThrottleThreshold;
    private final double densityThrottleChancePerMob;
    private final double densityThrottleMaxChance;
    private final boolean reproduceEnabled;
    private final double reproduceSpacing;
    private final double reproduceRate;
    private final int reproduceCooldownTicks;

    private MobSettings(
        double awarenessDistance,
        double herdRadius,
        int herdUpdateIntervalTicks,
        double fleeSpeed,
        double panicSpeed,
        double fleeRange,
        double panicRange,
        java.util.EnumSet<EntityType> predators,
        HerdType herdType,
        double nightHerdRadiusMultiplier,
        java.util.EnumSet<Material> grazeCrops,
        int grazeFrequencyTicks,
        int softCapPerBiome,
        int softCapPerChunk,
        int overcapIntervalTicks,
        int overcapRemovalsPerInterval,
        int minHerdSize,
        int densityThrottleThreshold,
        double densityThrottleChancePerMob,
        double densityThrottleMaxChance,
        boolean reproduceEnabled,
        double reproduceSpacing,
        double reproduceRate,
        int reproduceCooldownTicks) {
      this.awarenessDistance = awarenessDistance;
      this.herdRadius = herdRadius;
      this.herdUpdateIntervalTicks = herdUpdateIntervalTicks;
      this.fleeSpeed = fleeSpeed;
      this.panicSpeed = panicSpeed;
      this.fleeRange = fleeRange;
      this.panicRange = panicRange;
      this.predators = predators;
      this.herdType = herdType;
      this.nightHerdRadiusMultiplier = nightHerdRadiusMultiplier;
      this.grazeCrops = grazeCrops;
      this.grazeFrequencyTicks = grazeFrequencyTicks;
      this.softCapPerBiome = softCapPerBiome;
      this.softCapPerChunk = softCapPerChunk;
      this.overcapIntervalTicks = overcapIntervalTicks;
      this.overcapRemovalsPerInterval = overcapRemovalsPerInterval;
      this.minHerdSize = minHerdSize;
      this.densityThrottleThreshold = densityThrottleThreshold;
      this.densityThrottleChancePerMob = densityThrottleChancePerMob;
      this.densityThrottleMaxChance = densityThrottleMaxChance;
      this.reproduceEnabled = reproduceEnabled;
      this.reproduceSpacing = reproduceSpacing;
      this.reproduceRate = reproduceRate;
      this.reproduceCooldownTicks = reproduceCooldownTicks;
    }

    private static MobSettings fromConfig(FileConfiguration config, String basePath) {
      double awarenessDistance = getDouble(
          config,
          basePath + ".awareness-distance",
          basePath + ".awareness",
          24.0);
      double herdRadius = getDouble(
          config,
          basePath + ".herd-radius",
          basePath + ".herd-tightness",
          12.0);
      int herdUpdateIntervalTicks = config.getInt(basePath + ".herd-update-interval-ticks", 100);
      double fleeSpeed = config.getDouble(basePath + ".flee-speed", 1.1);
      double panicSpeed = config.getDouble(basePath + ".panic-speed", 1.5);
      double fleeRange = getDouble(
          config,
          basePath + ".flee-range",
          basePath + ".flee-distance",
          awarenessDistance * 0.6);
      double panicRange = getDouble(
          config,
          basePath + ".panic-range",
          basePath + ".panic-distance",
          awarenessDistance * 0.35);
      java.util.EnumSet<EntityType> predators = parsePredators(config, basePath + ".predators");
      HerdType herdType = HerdType.fromConfig(config.getString(basePath + ".herd-type", "loose"));
      double nightHerdRadiusMultiplier =
          config.getDouble(basePath + ".night-herd-radius-multiplier", 1.0);
      java.util.EnumSet<Material> grazeCrops =
          parseGrazeCrops(config, basePath + ".graze-crops");
      int grazeFrequencyTicks = Math.max(0, config.getInt(basePath + ".graze-frequency", 0));
      int softCapPerBiome = Math.max(0, config.getInt(basePath + ".soft-cap-per-biome", 0));
      int softCapPerChunk = Math.max(0, config.getInt(basePath + ".soft-cap-per-chunk", 0));
      int overcapIntervalTicks =
          Math.max(0, config.getInt(basePath + ".overcap-interval-ticks", 0));
      int overcapRemovalsPerInterval =
          Math.max(0, config.getInt(basePath + ".overcap-removals-per-interval", 0));
      int minHerdSize = Math.max(0, config.getInt(basePath + ".min-herd-size", 2));
      int densityThrottleThreshold =
          Math.max(0, config.getInt(basePath + ".density-throttle-threshold", 0));
      double densityThrottleChancePerMob =
          Math.max(0.0, config.getDouble(basePath + ".density-throttle-chance-per-mob", 0.0));
      double densityThrottleMaxChance =
          Math.max(0.0, config.getDouble(basePath + ".density-throttle-max-chance", 0.0));
      boolean reproduceEnabled = config.getBoolean(basePath + ".reproduce", false);
      double reproduceSpacing = Math.max(0.0, config.getDouble(basePath + ".reproduce-spacing", 3.0));
      double reproduceRate = Math.max(0.0, config.getDouble(basePath + ".reproduce-rate", 0.0));
      int reproduceCooldownTicks =
          Math.max(0, config.getInt(basePath + ".reproduce-cooldown-ticks", 6000));
      return new MobSettings(
          Math.max(0.0, awarenessDistance),
          Math.max(0.0, herdRadius),
          Math.max(0, herdUpdateIntervalTicks),
          Math.max(0.0, fleeSpeed),
          Math.max(0.0, panicSpeed),
          Math.max(0.0, fleeRange),
          Math.max(0.0, panicRange),
          predators,
          herdType,
          Math.max(0.0, nightHerdRadiusMultiplier),
          grazeCrops,
          grazeFrequencyTicks,
          softCapPerBiome,
          softCapPerChunk,
          overcapIntervalTicks,
          overcapRemovalsPerInterval,
          minHerdSize,
          densityThrottleThreshold,
          densityThrottleChancePerMob,
          densityThrottleMaxChance,
          reproduceEnabled,
          reproduceSpacing,
          reproduceRate,
          reproduceCooldownTicks);
    }

    private static double getDouble(
        FileConfiguration config,
        String primaryPath,
        String fallbackPath,
        double defaultValue) {
      if (config.contains(primaryPath)) {
        return config.getDouble(primaryPath, defaultValue);
      }
      if (config.contains(fallbackPath)) {
        return config.getDouble(fallbackPath, defaultValue);
      }
      return defaultValue;
    }

    private static java.util.EnumSet<Material> parseGrazeCrops(
        FileConfiguration config,
        String path) {
      java.util.EnumSet<Material> materials = java.util.EnumSet.noneOf(Material.class);
      if (!config.contains(path)) {
        return materials;
      }
      for (String entry : config.getStringList(path)) {
        Material material = Material.matchMaterial(entry);
        if (material != null) {
          materials.add(material);
        }
      }
      return materials;
    }

    private static java.util.EnumSet<EntityType> parsePredators(
        FileConfiguration config,
        String path) {
      java.util.EnumSet<EntityType> predators = java.util.EnumSet.noneOf(EntityType.class);
      if (!config.contains(path)) {
        return predators;
      }
      for (String entry : config.getStringList(path)) {
        if (entry == null || entry.isBlank()) {
          continue;
        }
        try {
          EntityType type = EntityType.valueOf(entry.trim().toUpperCase(Locale.ROOT));
          predators.add(type);
        } catch (IllegalArgumentException ignored) {
          // Skip invalid entity types from config.
        }
      }
      return predators;
    }
  }

  private static final class MobKind<T extends Mob> {
    private final EntityType entityType;
    private final Class<T> entityClass;
    private final String configKey;

    private MobKind(EntityType entityType, Class<T> entityClass, String configKey) {
      this.entityType = entityType;
      this.entityClass = entityClass;
      this.configKey = configKey;
    }
  }

  private static final class MobTypeConfig<T extends Mob> {
    private final EntityType entityType;
    private final Class<T> entityClass;
    private final MobSettings settings;

    private MobTypeConfig(
        EntityType entityType,
        Class<T> entityClass,
        MobSettings settings) {
      this.entityType = entityType;
      this.entityClass = entityClass;
      this.settings = settings;
    }
  }

  private enum HerdType {
    LOOSE,
    TIGHT;

    private static HerdType fromConfig(String value) {
      if (value == null) {
        return LOOSE;
      }
      String normalized = value.trim().toLowerCase(Locale.ROOT);
      if (normalized.equals("tight")) {
        return TIGHT;
      }
      return LOOSE;
    }
  }

  private final class HerdManager {
    private final Map<World, EnumMap<EntityType, HerdCache>> herdCaches = new HashMap<>();
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private static final int GRAZE_SCAN_ATTEMPTS = 12;
    private static final double GRAZE_REACH_DISTANCE_SQ = 2.5 * 2.5;
    private static final long GRAZE_CYCLE_TICKS = 24000L * 2L;
    private static final long GRAZE_WINDOW_START = 6000L;
    private static final long GRAZE_WINDOW_END = 9000L;
    private static final double AWARENESS_SPEED_MULTIPLIER = 0.6;
    private static final int PREDATOR_CHUNK_CACHE_TICKS = 2;
    private static final int PREDATOR_THREAT_TTL_TICKS = 3;
    private static final int PREDATOR_CHECK_INTERVAL_TICKS = 2;
    private static final int PREDATOR_CACHE_MAX_ENTRIES = 2048;
    private static final double DAY_LEADER_WANDER_CHANCE = 0.04;
    private static final double DAY_LEADER_WANDER_RADIUS_MULTIPLIER = 1.1;
    private static final double DAY_MEMBER_WANDER_CHANCE = 0.18;
    private static final double DAY_MEMBER_WANDER_RADIUS_MULTIPLIER = 1.6;
    private static final int DAY_MEMBER_WANDER_GRACE_TICKS = 120;
    private static final java.util.EnumSet<Material> NATURAL_BREEDING_BLOCKS =
        java.util.EnumSet.of(
            Material.GRASS_BLOCK,
            Material.DIRT,
            Material.COARSE_DIRT,
            Material.PODZOL,
            Material.MYCELIUM,
            Material.ROOTED_DIRT,
            Material.MOSS_BLOCK,
            Material.DIRT_PATH);
    private final Map<PredatorChunkKey, PredatorChunkCache> predatorChunkCache = new HashMap<>();

    private void updateHerdsIfDue(
        World world,
        MobTypeConfig<? extends Mob> config,
        int currentTick,
        DebugSink debugSink) {
      HerdCache cache = getCache(world, config.entityType);
      if (currentTick - cache.lastUpdateTick < config.settings.herdUpdateIntervalTicks) {
        return;
      }
      List<Mob> mobs = new ArrayList<>(world.getEntitiesByClass(config.entityClass));
      List<HerdCluster> clusters = buildClusters(mobs, config.settings.awarenessDistance);
      Map<Biome, Integer> biomeCounts = countByBiome(mobs);
      Map<Long, Integer> chunkCounts = countByChunk(mobs);

      Map<UUID, UUID> newMemberToLeader = new HashMap<>();
      Map<UUID, Integer> newLeaderFollowerCounts = new HashMap<>();
      List<HerdSnapshot> newSnapshots = new ArrayList<>();

      boolean day = isDay(world);
      for (HerdCluster cluster : clusters) {
        Mob leader = electLeader(cluster, cache);
        if (leader == null) {
          continue;
        }
        PredatorThreat threat = resolvePredatorThreat(leader, config.settings, cache, currentTick);
        UUID leaderId = leader.getUniqueId();
        newLeaderFollowerCounts.put(leaderId, cluster.members.size());
        Location leaderLocation = leader.getLocation();
        HerdType herdType = config.settings.herdType;
        double baseRadius = config.settings.herdRadius;
        double followRadius = baseRadius;
        if (herdType == HerdType.TIGHT) {
          followRadius *= day ? 1.0 : 0.85;
        } else {
          followRadius *= day ? 1.8 : 1.4;
        }
        if (!day) {
          followRadius *= config.settings.nightHerdRadiusMultiplier;
        }
        Location trailingTarget = resolveTrailingTarget(leaderLocation, baseRadius, herdType);
        if (leaderLocation != null) {
          newSnapshots.add(new HerdSnapshot(
              config.entityType,
              leaderId,
              leaderLocation,
              cluster.members.size(),
              leaderLocation.getBlock().getBiome()));
        }
        for (Mob member : cluster.members) {
          if (member == null || !member.isValid()) {
            continue;
          }
          newMemberToLeader.put(member.getUniqueId(), leaderId);
        }
        if (threat != null) {
          fleeFromPredator(cluster, threat, config.settings);
          continue;
        }
        if (day && random.nextDouble() < DAY_LEADER_WANDER_CHANCE) {
          double leaderWanderRadius = followRadius * DAY_LEADER_WANDER_RADIUS_MULTIPLIER;
          Location wanderTarget = resolveDayWanderTarget(leaderLocation, leaderWanderRadius);
          if (wanderTarget != null) {
            leader.getPathfinder().moveTo(wanderTarget, config.settings.fleeSpeed);
          }
        }
        applyPopulationControl(
            cluster,
            leader,
            config.entityType,
            config.settings,
            cache,
            currentTick,
            biomeCounts,
            chunkCounts,
            debugSink);
        attemptNaturalReproduction(
            cluster,
            leader,
            config.entityType,
            config.settings,
            cache,
            currentTick,
            biomeCounts,
            chunkCounts);
        double radiusSq = followRadius * followRadius;
        double dayWanderRadius = followRadius * DAY_MEMBER_WANDER_RADIUS_MULTIPLIER;
        double dayWanderRadiusSq = dayWanderRadius * dayWanderRadius;
        for (Mob member : cluster.members) {
          if (member == null || !member.isValid()) {
            continue;
          }
          UUID memberId = member.getUniqueId();
          if (memberId.equals(leaderId)) {
            continue;
          }
          if (day
              && random.nextDouble() < DAY_MEMBER_WANDER_CHANCE
              && member.getLocation().distanceSquared(leaderLocation) <= dayWanderRadiusSq) {
            double roamRadius = followRadius * DAY_MEMBER_WANDER_RADIUS_MULTIPLIER;
            Location wanderTarget = resolveDayWanderTarget(leaderLocation, roamRadius);
            if (wanderTarget != null) {
              member.getPathfinder().moveTo(wanderTarget, config.settings.fleeSpeed);
              cache.lastMemberWanderTicks.put(memberId, currentTick);
              continue;
            }
          }
          if (day) {
            int lastWanderTick = cache.lastMemberWanderTicks.getOrDefault(memberId, -1);
            if (lastWanderTick > 0
                && currentTick - lastWanderTick <= DAY_MEMBER_WANDER_GRACE_TICKS) {
              continue;
            }
          }
          if (member.getLocation().distanceSquared(leaderLocation) > radiusSq) {
            Location target = herdType == HerdType.TIGHT ? trailingTarget : leaderLocation;
            if (target == null) {
              target = leaderLocation;
            }
            if (target != null) {
              member.getPathfinder().moveTo(target, config.settings.fleeSpeed);
            }
          }
        }
        handleLeaderGrazing(world, leader, cluster, config, cache, currentTick);
      }

      cache.memberToLeader = newMemberToLeader;
      cache.leaderFollowerCounts = newLeaderFollowerCounts;
      cache.snapshots = newSnapshots;
      cache.lastUpdateTick = currentTick;
    }

    private PredatorThreat resolvePredatorThreat(
        Mob leader,
        MobSettings settings,
        HerdCache cache,
        int currentTick) {
      if (leader == null || !leader.isValid()) {
        return null;
      }
      UUID leaderId = leader.getUniqueId();
      PredatorThreat panicThreat = findPanicThreat(leader, settings);
      if (panicThreat != null) {
        cache.lastThreats.put(
            leaderId,
            new PredatorThreatState(panicThreat.predatorLocation, currentTick));
        return panicThreat;
      }
      boolean shouldCheck = shouldCheckPredators(currentTick, leaderId);
      if (shouldCheck) {
        PredatorThreat threat = findPredatorThreat(leader, settings, currentTick);
        if (threat != null) {
          cache.lastThreats.put(
              leaderId,
              new PredatorThreatState(threat.predatorLocation, currentTick));
        } else {
          cache.lastThreats.remove(leaderId);
        }
        return threat;
      }
      PredatorThreatState cached = cache.lastThreats.get(leaderId);
      if (cached == null) {
        return null;
      }
      if (currentTick - cached.lastSeenTick > PREDATOR_THREAT_TTL_TICKS) {
        cache.lastThreats.remove(leaderId);
        return null;
      }
      return new PredatorThreat(cached.predatorLocation);
    }

    private PredatorThreat findPanicThreat(Mob leader, MobSettings settings) {
      if (leader == null || !leader.isValid()) {
        return null;
      }
      if (settings.predators.isEmpty()) {
        return null;
      }
      double panicRange = settings.panicRange;
      if (panicRange <= 0.0) {
        return null;
      }
      Location leaderLocation = leader.getLocation();
      double panicRangeSq = panicRange * panicRange;
      for (Entity entity : leader.getNearbyEntities(panicRange, panicRange, panicRange)) {
        if (entity == null || !entity.isValid()) {
          continue;
        }
        EntityType type = entity.getType();
        if (!settings.predators.contains(type)) {
          continue;
        }
        if (entity instanceof Player player && (!isSurvivalPlayer(player) || player.isDead())) {
          logIgnoredPredator(player, "panic threat scan");
          continue;
        }
        double distanceSq = leaderLocation.distanceSquared(entity.getLocation());
        if (distanceSq <= panicRangeSq) {
          return new PredatorThreat(entity.getLocation());
        }
      }
      return null;
    }

    private boolean shouldCheckPredators(int currentTick, UUID leaderId) {
      if (PREDATOR_CHECK_INTERVAL_TICKS <= 1) {
        return true;
      }
      int hash = leaderId != null ? leaderId.hashCode() : 0;
      int offset = Math.floorMod(hash, PREDATOR_CHECK_INTERVAL_TICKS);
      return Math.floorMod(currentTick + offset, PREDATOR_CHECK_INTERVAL_TICKS) == 0;
    }

    private HerdCache getCache(World world, EntityType type) {
      return herdCaches
          .computeIfAbsent(world, ignored -> new EnumMap<>(EntityType.class))
          .computeIfAbsent(type, ignored -> new HerdCache());
    }

    private Map<Biome, Integer> countByBiome(List<? extends Mob> mobs) {
      Map<Biome, Integer> counts = new HashMap<>();
      for (Mob mob : mobs) {
        if (mob == null || !mob.isValid()) {
          continue;
        }
        Biome biome = mob.getLocation().getBlock().getBiome();
        counts.merge(biome, 1, Integer::sum);
      }
      return counts;
    }

    private Map<Long, Integer> countByChunk(List<? extends Mob> mobs) {
      Map<Long, Integer> counts = new HashMap<>();
      for (Mob mob : mobs) {
        if (mob == null || !mob.isValid()) {
          continue;
        }
        var chunk = mob.getLocation().getChunk();
        long key = chunkKey(chunk.getX(), chunk.getZ());
        counts.merge(key, 1, Integer::sum);
      }
      return counts;
    }

    private long chunkKey(int chunkX, int chunkZ) {
      return (((long) chunkX) << 32) ^ (chunkZ & 0xffffffffL);
    }

    private List<HerdCluster> buildClusters(List<? extends Mob> mobs, double awarenessDistance) {
      double distanceSq = awarenessDistance * awarenessDistance;
      List<HerdCluster> clusters = new ArrayList<>();
      boolean[] assigned = new boolean[mobs.size()];
      for (int i = 0; i < mobs.size(); i++) {
        if (assigned[i]) {
          continue;
        }
        Mob seed = mobs.get(i);
        if (seed == null || !seed.isValid()) {
          assigned[i] = true;
          continue;
        }
        List<Mob> members = new ArrayList<>();
        Deque<Integer> queue = new ArrayDeque<>();
        queue.add(i);
        assigned[i] = true;
        while (!queue.isEmpty()) {
          int index = queue.removeFirst();
          Mob current = mobs.get(index);
          if (current == null || !current.isValid()) {
            continue;
          }
          members.add(current);
          Location currentLocation = current.getLocation();
          for (int j = 0; j < mobs.size(); j++) {
            if (assigned[j]) {
              continue;
            }
            Mob other = mobs.get(j);
            if (other == null || !other.isValid()) {
              assigned[j] = true;
              continue;
            }
            if (currentLocation.distanceSquared(other.getLocation()) <= distanceSq) {
              assigned[j] = true;
              queue.add(j);
            }
          }
        }
        if (!members.isEmpty()) {
          clusters.add(new HerdCluster(members));
        }
      }
      return clusters;
    }

    private Mob electLeader(HerdCluster cluster, HerdCache cache) {
      if (cluster.members.isEmpty()) {
        return null;
      }
      List<Mob> candidates = new ArrayList<>();
      for (Mob member : cluster.members) {
        if (member == null || !member.isValid()) {
          continue;
        }
        UUID memberId = member.getUniqueId();
        UUID previousLeader = cache.memberToLeader.get(memberId);
        if (previousLeader != null && previousLeader.equals(memberId)) {
          candidates.add(member);
        }
      }
      if (candidates.isEmpty()) {
        return cluster.members.get(random.nextInt(cluster.members.size()));
      }
      int bestCount = -1;
      List<Mob> bestLeaders = new ArrayList<>();
      for (Mob candidate : candidates) {
        UUID candidateId = candidate.getUniqueId();
        int count = cache.leaderFollowerCounts.getOrDefault(candidateId, 1);
        if (count > bestCount) {
          bestCount = count;
          bestLeaders.clear();
          bestLeaders.add(candidate);
        } else if (count == bestCount) {
          bestLeaders.add(candidate);
        }
      }
      return bestLeaders.get(random.nextInt(bestLeaders.size()));
    }

    private Location resolveTrailingTarget(
        Location leaderLocation,
        double herdRadius,
        HerdType herdType) {
      if (leaderLocation == null) {
        return null;
      }
      if (herdRadius <= 0.0) {
        return leaderLocation;
      }
      org.bukkit.util.Vector direction = leaderLocation.getDirection();
      if (direction.lengthSquared() < 0.001) {
        return leaderLocation;
      }
      double trailingDistance = herdRadius * (herdType == HerdType.TIGHT ? 0.35 : 0.75);
      return leaderLocation.clone().subtract(direction.normalize().multiply(trailingDistance));
    }

    private Location resolveDayWanderTarget(Location leaderLocation, double radius) {
      if (leaderLocation == null || radius <= 0.0) {
        return null;
      }
      double angle = random.nextDouble(0.0, Math.PI * 2.0);
      double distance = random.nextDouble(0.35, 1.0) * radius;
      double dx = Math.cos(angle) * distance;
      double dz = Math.sin(angle) * distance;
      Location target = leaderLocation.clone().add(dx, 0.0, dz);
      target.setY(leaderLocation.getY());
      return target;
    }

    private boolean isDay(World world) {
      long time = world.getTime() % 24000L;
      return time >= 0L && time < 12300L;
    }

    private void handleLeaderGrazing(
        World world,
        Mob leader,
        HerdCluster cluster,
        MobTypeConfig<? extends Mob> config,
        HerdCache cache,
        int currentTick) {
      if (leader == null || !leader.isValid()) {
        return;
      }
      if (config.settings.grazeCrops.isEmpty()) {
        return;
      }
      Location grazeTarget = findGrazeTarget(world, leader.getLocation(), config.settings);
      if (grazeTarget == null) {
        return;
      }
      leader.getPathfinder().moveTo(grazeTarget, config.settings.fleeSpeed);
      if (!isGrazeWindowOpen(world)) {
        return;
      }
      int grazeFrequencyTicks = config.settings.grazeFrequencyTicks;
      if (leader.getLocation().distanceSquared(grazeTarget) <= GRAZE_REACH_DISTANCE_SQ) {
        if (isGrazeReady(leader.getUniqueId(), cache, grazeFrequencyTicks, currentTick)
            && tryEatCropAt(grazeTarget, config.settings.grazeCrops)) {
          cache.lastGrazeTicks.put(leader.getUniqueId(), currentTick);
        }
      }
      for (Mob member : cluster.members) {
        if (member == null || !member.isValid()) {
          continue;
        }
        if (member.getLocation().distanceSquared(grazeTarget) <= GRAZE_REACH_DISTANCE_SQ) {
          UUID memberId = member.getUniqueId();
          if (isGrazeReady(memberId, cache, grazeFrequencyTicks, currentTick)
              && tryEatCropAt(grazeTarget, config.settings.grazeCrops)) {
            cache.lastGrazeTicks.put(memberId, currentTick);
          }
        }
      }
    }

    private PredatorThreat findPredatorThreat(
        Mob leader,
        MobSettings settings,
        int currentTick) {
      if (leader == null || !leader.isValid()) {
        return null;
      }
      if (settings.predators.isEmpty()) {
        return null;
      }
      double awareness = settings.awarenessDistance;
      if (awareness <= 0.0) {
        return null;
      }
      Location leaderLocation = leader.getLocation();
      double bestDistanceSq = awareness * awareness;
      Location bestLocation = null;
      double panicRangeSq = settings.panicRange * settings.panicRange;
      int chunkRadius = (int) Math.ceil(awareness / 16.0);
      int originChunkX = leaderLocation.getBlockX() >> 4;
      int originChunkZ = leaderLocation.getBlockZ() >> 4;
      for (int dx = -chunkRadius; dx <= chunkRadius; dx++) {
        for (int dz = -chunkRadius; dz <= chunkRadius; dz++) {
          int chunkX = originChunkX + dx;
          int chunkZ = originChunkZ + dz;
          if (!leader.getWorld().isChunkLoaded(chunkX, chunkZ)) {
            continue;
          }
          PredatorChunkCache cached = getPredatorChunkCache(
              leader.getWorld(),
              chunkX,
              chunkZ,
              settings.predators,
              currentTick);
          for (Entity entity : cached.entities) {
            if (entity == null || !entity.isValid()) {
              continue;
            }
            EntityType type = entity.getType();
            if (!settings.predators.contains(type)) {
              continue;
            }
            if (entity instanceof Player player && (!isSurvivalPlayer(player) || player.isDead())) {
              logIgnoredPredator(player, "predator threat scan");
              continue;
            }
            double distanceSq = leaderLocation.distanceSquared(entity.getLocation());
            if (distanceSq > bestDistanceSq) {
              continue;
            }
            bestDistanceSq = distanceSq;
            bestLocation = entity.getLocation();
            if (panicRangeSq > 0.0 && bestDistanceSq <= panicRangeSq) {
              return new PredatorThreat(bestLocation);
            }
          }
        }
      }
      if (bestLocation == null) {
        return null;
      }
      return new PredatorThreat(bestLocation);
    }

    private PredatorChunkCache getPredatorChunkCache(
        World world,
        int chunkX,
        int chunkZ,
        java.util.EnumSet<EntityType> predatorTypes,
        int currentTick) {
      if (predatorChunkCache.size() > PREDATOR_CACHE_MAX_ENTRIES) {
        prunePredatorChunkCache(currentTick);
      }
      int predatorSignature = predatorTypes.hashCode();
      PredatorChunkKey key = new PredatorChunkKey(
          world.getUID(),
          chunkX,
          chunkZ,
          predatorSignature);
      PredatorChunkCache cached = predatorChunkCache.get(key);
      if (cached != null && currentTick - cached.lastScanTick <= PREDATOR_CHUNK_CACHE_TICKS) {
        return cached;
      }
      Chunk chunk = world.getChunkAt(chunkX, chunkZ);
      Entity[] entities = chunk.getEntities();
      List<Entity> predatorEntities = new ArrayList<>();
      for (Entity entity : entities) {
        if (entity == null || !entity.isValid()) {
          continue;
        }
        if (entity instanceof Player player && (!isSurvivalPlayer(player) || player.isDead())) {
          logIgnoredPredator(player, "predator chunk cache");
          continue;
        }
        if (predatorTypes.contains(entity.getType())) {
          predatorEntities.add(entity);
        }
      }
      PredatorChunkCache refreshed = new PredatorChunkCache(predatorEntities, currentTick);
      predatorChunkCache.put(key, refreshed);
      return refreshed;
    }

    private void prunePredatorChunkCache(int currentTick) {
      int staleAfter = PREDATOR_CHUNK_CACHE_TICKS * 2;
      predatorChunkCache.entrySet().removeIf(
          entry -> currentTick - entry.getValue().lastScanTick > staleAfter);
    }

    private void fleeFromPredator(
        HerdCluster cluster,
        PredatorThreat threat,
        MobSettings settings) {
      if (cluster == null || threat == null) {
        return;
      }
      double awareness = settings.awarenessDistance;
      if (awareness <= 0.0) {
        return;
      }
      double fleeRangeSq = settings.fleeRange * settings.fleeRange;
      double panicRangeSq = settings.panicRange * settings.panicRange;
      double awarenessSpeed = settings.fleeSpeed * AWARENESS_SPEED_MULTIPLIER;
      for (Mob member : cluster.members) {
        if (member == null || !member.isValid()) {
          continue;
        }
        Location memberLocation = member.getLocation();
        double predatorDistanceSq = memberLocation.distanceSquared(threat.predatorLocation);
        double speed = awarenessSpeed;
        if (panicRangeSq > 0.0 && predatorDistanceSq <= panicRangeSq) {
          speed = settings.panicSpeed;
        } else if (fleeRangeSq > 0.0 && predatorDistanceSq <= fleeRangeSq) {
          speed = settings.fleeSpeed;
        }
        org.bukkit.util.Vector away = memberLocation.toVector()
            .subtract(threat.predatorLocation.toVector());
        if (away.lengthSquared() < 0.001) {
          away = new org.bukkit.util.Vector(
              random.nextDouble(-1.0, 1.0),
              0.0,
              random.nextDouble(-1.0, 1.0));
        }
        Location target = memberLocation.clone()
            .add(away.normalize().multiply(awareness));
        member.getPathfinder().moveTo(target, speed);
      }
    }

    private void triggerImmediateFlee(
        Mob prey,
        Location predatorLocation,
        MobSettings settings,
        int currentTick) {
      if (prey == null || !prey.isValid() || predatorLocation == null) {
        return;
      }
      double awareness = settings.awarenessDistance;
      if (awareness <= 0.0) {
        return;
      }
      List<Mob> members = new ArrayList<>();
      members.add(prey);
      for (Entity entity : prey.getNearbyEntities(awareness, awareness, awareness)) {
        if (!(entity instanceof Mob mob)) {
          continue;
        }
        if (!mob.isValid() || mob.getType() != prey.getType()) {
          continue;
        }
        if (mob.getUniqueId().equals(prey.getUniqueId())) {
          continue;
        }
        members.add(mob);
      }
      if (members.isEmpty()) {
        return;
      }
      HerdCache cache = getCache(prey.getWorld(), prey.getType());
      UUID leaderId = cache.memberToLeader.getOrDefault(prey.getUniqueId(), prey.getUniqueId());
      cache.lastThreats.put(leaderId, new PredatorThreatState(predatorLocation, currentTick));
      fleeFromPredator(new HerdCluster(members), new PredatorThreat(predatorLocation), settings);
    }

    private Location findGrazeTarget(World world, Location origin, MobSettings settings) {
      if (origin == null || world == null) {
        return null;
      }
      double radius = settings.awarenessDistance;
      if (radius <= 0.0) {
        return null;
      }
      int originX = origin.getBlockX();
      int originZ = origin.getBlockZ();
      for (int i = 0; i < GRAZE_SCAN_ATTEMPTS; i++) {
        double angle = random.nextDouble(0.0, Math.PI * 2.0);
        double distance = random.nextDouble(0.0, radius);
        int targetX = originX + (int) Math.round(Math.cos(angle) * distance);
        int targetZ = originZ + (int) Math.round(Math.sin(angle) * distance);
        var ground = world.getHighestBlockAt(
            targetX,
            targetZ,
            HeightMap.MOTION_BLOCKING_NO_LEAVES);
        if (ground == null) {
          continue;
        }
        if (settings.grazeCrops.contains(ground.getType())) {
          return ground.getLocation();
        }
        var above = ground.getRelative(org.bukkit.block.BlockFace.UP);
        if (settings.grazeCrops.contains(above.getType())) {
          return above.getLocation();
        }
      }
      return null;
    }

    private boolean isGrazeWindowOpen(World world) {
      long time = world.getFullTime() % GRAZE_CYCLE_TICKS;
      return time >= GRAZE_WINDOW_START && time <= GRAZE_WINDOW_END;
    }

    private void applyPopulationControl(
        HerdCluster cluster,
        Mob leader,
        EntityType entityType,
        MobSettings settings,
        HerdCache cache,
        int currentTick,
        Map<Biome, Integer> biomeCounts,
        Map<Long, Integer> chunkCounts,
        DebugSink debugSink) {
      if (cluster == null || leader == null || !leader.isValid()) {
        return;
      }
      int clusterSize = cluster.members.size();
      if (clusterSize <= settings.minHerdSize) {
        return;
      }
      CullPlan softCapPlan = resolveSoftCapRemovals(
          leader,
          settings,
          cache,
          currentTick,
          biomeCounts,
          chunkCounts);
      boolean culled = false;
      if (softCapPlan.removals > 0) {
        int removed = cullMembers(
            cluster,
            leader.getUniqueId(),
            softCapPlan.removals,
            settings.minHerdSize,
            biomeCounts,
            chunkCounts);
        if (removed > 0) {
          cache.lastCullTicks.put(leader.getUniqueId(), currentTick);
          logCull(
              debugSink,
              removed,
              entityType,
              softCapPlan.reason == CullReason.SOFT_CAP_CHUNK
                  ? "soft cap per chunk"
                  : "soft cap per biome");
          culled = true;
        }
      }
      if (culled) {
        return;
      }
      int threshold = settings.densityThrottleThreshold;
      if (threshold <= 0) {
        return;
      }
      int excess = cluster.members.size() - threshold;
      if (excess <= 0) {
        return;
      }
      double chance =
          Math.min(
              settings.densityThrottleMaxChance,
              excess * settings.densityThrottleChancePerMob);
      if (chance <= 0.0) {
        return;
      }
      if (random.nextDouble() > chance) {
        return;
      }
      int removed = cullMembers(
          cluster,
          leader.getUniqueId(),
          1,
          settings.minHerdSize,
          biomeCounts,
          chunkCounts);
      if (removed > 0) {
        logCull(debugSink, removed, entityType, "density throttle");
      }
    }

    private void attemptNaturalReproduction(
        HerdCluster cluster,
        Mob leader,
        EntityType entityType,
        MobSettings settings,
        HerdCache cache,
        int currentTick,
        Map<Biome, Integer> biomeCounts,
        Map<Long, Integer> chunkCounts) {
      if (!settings.reproduceEnabled || settings.reproduceRate <= 0.0) {
        return;
      }
      if (cluster == null || leader == null || !leader.isValid()) {
        return;
      }
      if (isOverCap(leader, settings, biomeCounts, chunkCounts)) {
        return;
      }
      if (settings.densityThrottleThreshold > 0
          && cluster.members.size() > settings.densityThrottleThreshold) {
        return;
      }
      if (random.nextDouble() > settings.reproduceRate) {
        return;
      }
      double spacingSq = settings.reproduceSpacing * settings.reproduceSpacing;
      List<Mob> eligible = new ArrayList<>();
      for (Mob member : cluster.members) {
        if (isEligibleParent(member, settings, cache, currentTick)) {
          eligible.add(member);
        }
      }
      if (eligible.size() < 2) {
        return;
      }
      for (int attempt = 0; attempt < Math.min(6, eligible.size()); attempt++) {
        Mob parentA = eligible.get(random.nextInt(eligible.size()));
        Location locationA = parentA.getLocation();
        Mob parentB = null;
        for (Mob candidate : eligible) {
          if (candidate == parentA) {
            continue;
          }
          if (locationA.distanceSquared(candidate.getLocation()) <= spacingSq) {
            parentB = candidate;
            break;
          }
        }
        if (parentB == null) {
          continue;
        }
        Location spawnLocation = averageLocation(parentA.getLocation(), parentB.getLocation());
        if (spawnLocation == null) {
          return;
        }
        Entity babyEntity = leader.getWorld().spawnEntity(spawnLocation, entityType);
        if (babyEntity instanceof Ageable baby) {
          baby.setBaby();
          cache.lastReproduceTicks.put(parentA.getUniqueId(), currentTick);
          cache.lastReproduceTicks.put(parentB.getUniqueId(), currentTick);
        } else {
          babyEntity.remove();
        }
        return;
      }
    }

    private boolean isEligibleParent(
        Mob member,
        MobSettings settings,
        HerdCache cache,
        int currentTick) {
      if (member == null || !member.isValid()) {
        return false;
      }
      if (!(member instanceof Ageable ageable) || !ageable.isAdult()) {
        return false;
      }
      if (isInWater(member)) {
        return false;
      }
      if (!isOnNaturalSurface(member.getLocation())) {
        return false;
      }
      int cooldown = settings.reproduceCooldownTicks;
      if (cooldown <= 0) {
        return true;
      }
      int lastTick = cache.lastReproduceTicks.getOrDefault(member.getUniqueId(), -cooldown);
      return currentTick - lastTick >= cooldown;
    }

    private boolean isInWater(Mob member) {
      Location location = member.getLocation();
      return location.getBlock().isLiquid()
          || location.getBlock().getRelative(org.bukkit.block.BlockFace.DOWN).isLiquid();
    }

    private boolean isOnNaturalSurface(Location location) {
      if (location == null) {
        return false;
      }
      var block = location.getBlock();
      var below = block.getRelative(org.bukkit.block.BlockFace.DOWN);
      if (!NATURAL_BREEDING_BLOCKS.contains(below.getType())) {
        return false;
      }
      World world = location.getWorld();
      if (world == null) {
        return false;
      }
      int surfaceY = world.getHighestBlockYAt(location);
      return below.getY() >= surfaceY - 1;
    }

    private Location averageLocation(Location a, Location b) {
      if (a == null || b == null || a.getWorld() != b.getWorld()) {
        return null;
      }
      Vector mid = a.toVector().add(b.toVector()).multiply(0.5);
      return new Location(a.getWorld(), mid.getX(), mid.getY(), mid.getZ());
    }

    private boolean isOverCap(
        Mob leader,
        MobSettings settings,
        Map<Biome, Integer> biomeCounts,
        Map<Long, Integer> chunkCounts) {
      if (settings.softCapPerBiome <= 0 && settings.softCapPerChunk <= 0) {
        return false;
      }
      if (leader == null || !leader.isValid()) {
        return false;
      }
      int biomeOver = 0;
      if (settings.softCapPerBiome > 0) {
        Biome biome = leader.getLocation().getBlock().getBiome();
        int count = biomeCounts.getOrDefault(biome, 0);
        biomeOver = count - settings.softCapPerBiome;
      }
      int chunkOver = 0;
      if (settings.softCapPerChunk > 0) {
        var chunk = leader.getLocation().getChunk();
        long key = chunkKey(chunk.getX(), chunk.getZ());
        int count = chunkCounts.getOrDefault(key, 0);
        chunkOver = count - settings.softCapPerChunk;
      }
      return Math.max(biomeOver, chunkOver) > 0;
    }

    private void logIgnoredPredator(Player player, String context) {
      if (player == null) {
        return;
      }
      logDebug(String.format(
          "TrophicHerds ignored player predator %s (%s) during %s.",
          player.getName(),
          player.getGameMode(),
          context));
    }

    private CullPlan resolveSoftCapRemovals(
        Mob leader,
        MobSettings settings,
        HerdCache cache,
        int currentTick,
        Map<Biome, Integer> biomeCounts,
        Map<Long, Integer> chunkCounts) {
      if (settings.overcapIntervalTicks <= 0 || settings.overcapRemovalsPerInterval <= 0) {
        return CullPlan.none();
      }
      if (settings.softCapPerBiome <= 0 && settings.softCapPerChunk <= 0) {
        return CullPlan.none();
      }
      UUID leaderId = leader.getUniqueId();
      int lastCullTick = cache.lastCullTicks.getOrDefault(leaderId, -settings.overcapIntervalTicks);
      if (currentTick - lastCullTick < settings.overcapIntervalTicks) {
        return CullPlan.none();
      }
      int biomeOver = 0;
      if (settings.softCapPerBiome > 0) {
        Biome biome = leader.getLocation().getBlock().getBiome();
        int count = biomeCounts.getOrDefault(biome, 0);
        biomeOver = count - settings.softCapPerBiome;
      }
      int chunkOver = 0;
      if (settings.softCapPerChunk > 0) {
        var chunk = leader.getLocation().getChunk();
        long key = chunkKey(chunk.getX(), chunk.getZ());
        int count = chunkCounts.getOrDefault(key, 0);
        chunkOver = count - settings.softCapPerChunk;
      }
      int overcap = Math.max(biomeOver, chunkOver);
      if (overcap <= 0) {
        return CullPlan.none();
      }
      CullReason reason =
          chunkOver >= biomeOver && chunkOver > 0
              ? CullReason.SOFT_CAP_CHUNK
              : CullReason.SOFT_CAP_BIOME;
      int removals = Math.min(settings.overcapRemovalsPerInterval, overcap);
      return new CullPlan(removals, reason);
    }

    private void logCull(
        DebugSink debugSink,
        int removed,
        EntityType entityType,
        String reason) {
      if (debugSink == null || removed <= 0 || entityType == null) {
        return;
      }
      String message = String.format(
          "TrophicHerds culled %d %s due to %s.",
          removed,
          entityType.name().toLowerCase(Locale.ROOT),
          reason);
      debugSink.log(message);
    }

    private int cullMembers(
        HerdCluster cluster,
        UUID leaderId,
        int removals,
        int minHerdSize,
        Map<Biome, Integer> biomeCounts,
        Map<Long, Integer> chunkCounts) {
      if (removals <= 0 || cluster.members.isEmpty()) {
        return 0;
      }
      List<Mob> candidates = new ArrayList<>();
      for (Mob member : cluster.members) {
        if (member == null || !member.isValid()) {
          continue;
        }
        if (leaderId != null && leaderId.equals(member.getUniqueId())) {
          continue;
        }
        candidates.add(member);
      }
      if (candidates.isEmpty()
          && leaderId != null
          && cluster.members.size() - 1 >= minHerdSize) {
        for (Mob member : cluster.members) {
          if (member != null && member.isValid() && leaderId.equals(member.getUniqueId())) {
            candidates.add(member);
            break;
          }
        }
      }
      int removed = 0;
      while (removed < removals && cluster.members.size() > minHerdSize && !candidates.isEmpty()) {
        int index = random.nextInt(candidates.size());
        Mob target = candidates.remove(index);
        if (target == null || !target.isValid()) {
          continue;
        }
        Location location = target.getLocation();
        Biome biome = location.getBlock().getBiome();
        biomeCounts.computeIfPresent(biome, (key, value) -> Math.max(0, value - 1));
        var chunk = location.getChunk();
        long key = chunkKey(chunk.getX(), chunk.getZ());
        chunkCounts.computeIfPresent(key, (k, value) -> Math.max(0, value - 1));
        target.remove();
        cluster.members.remove(target);
        removed++;
      }
      return removed;
    }

    private boolean isGrazeReady(
        UUID mobId,
        HerdCache cache,
        int grazeFrequencyTicks,
        int currentTick) {
      if (grazeFrequencyTicks <= 0) {
        return true;
      }
      int lastGrazeTick = cache.lastGrazeTicks.getOrDefault(mobId, -grazeFrequencyTicks);
      return currentTick - lastGrazeTick >= grazeFrequencyTicks;
    }

    private boolean tryEatCropAt(Location location, java.util.EnumSet<Material> grazeCrops) {
      var block = location.getBlock();
      if (grazeCrops.contains(block.getType())) {
        block.setType(Material.AIR);
        return true;
      }
      var below = block.getRelative(org.bukkit.block.BlockFace.DOWN);
      if (grazeCrops.contains(below.getType())) {
        below.setType(Material.AIR);
        return true;
      }
      return false;
    }

    List<HerdSnapshot> getHerdSnapshots(
        World world,
        Biome biome,
        Location reference,
        double radius,
        int minPopulation) {
      EnumMap<EntityType, HerdCache> worldCaches = herdCaches.get(world);
      if (worldCaches == null) {
        return List.of();
      }
      double radiusSq = radius > 0.0 ? radius * radius : -1.0;
      List<HerdSnapshot> results = new ArrayList<>();
      for (HerdCache cache : worldCaches.values()) {
        if (cache.snapshots == null) {
          continue;
        }
        for (HerdSnapshot snapshot : cache.snapshots) {
          if (snapshot.population < minPopulation) {
            continue;
          }
          if (biome != null && snapshot.biome != biome) {
            continue;
          }
          if (radiusSq > 0.0
              && reference != null
              && snapshot.leaderLocation.getWorld() == reference.getWorld()
              && snapshot.leaderLocation.distanceSquared(reference) > radiusSq) {
            continue;
          }
          results.add(snapshot);
        }
      }
      return results;
    }
  }

  private static final class HerdCache {
    private Map<UUID, UUID> memberToLeader = new HashMap<>();
    private Map<UUID, Integer> leaderFollowerCounts = new HashMap<>();
    private Map<UUID, PredatorThreatState> lastThreats = new HashMap<>();
    private Map<UUID, Integer> lastGrazeTicks = new HashMap<>();
    private Map<UUID, Integer> lastCullTicks = new HashMap<>();
    private Map<UUID, Integer> lastMemberWanderTicks = new HashMap<>();
    private Map<UUID, Integer> lastReproduceTicks = new HashMap<>();
    private List<HerdSnapshot> snapshots = List.of();
    private int lastUpdateTick;
  }

  private enum CullReason {
    SOFT_CAP_BIOME,
    SOFT_CAP_CHUNK
  }

  private static final class CullPlan {
    private final int removals;
    private final CullReason reason;

    private CullPlan(int removals, CullReason reason) {
      this.removals = removals;
      this.reason = reason;
    }

    private static CullPlan none() {
      return new CullPlan(0, CullReason.SOFT_CAP_BIOME);
    }
  }

  private static final class HerdCluster {
    private final List<Mob> members;

    private HerdCluster(List<Mob> members) {
      this.members = members;
    }
  }

  private static final class HerdSnapshot {
    private final EntityType entityType;
    private final UUID leaderId;
    private final Location leaderLocation;
    private final int population;
    private final Biome biome;

    private HerdSnapshot(
        EntityType entityType,
        UUID leaderId,
        Location leaderLocation,
        int population,
        Biome biome) {
      this.entityType = entityType;
      this.leaderId = leaderId;
      this.leaderLocation = leaderLocation;
      this.population = population;
      this.biome = biome;
    }
  }

  private static final class PredatorThreat {
    private final Location predatorLocation;

    private PredatorThreat(Location predatorLocation) {
      this.predatorLocation = predatorLocation;
    }
  }

  private static final class PredatorThreatState {
    private final Location predatorLocation;
    private final int lastSeenTick;

    private PredatorThreatState(Location predatorLocation, int lastSeenTick) {
      this.predatorLocation = predatorLocation;
      this.lastSeenTick = lastSeenTick;
    }
  }

  private static final class PredatorChunkCache {
    private final List<Entity> entities;
    private final int lastScanTick;

    private PredatorChunkCache(List<Entity> entities, int lastScanTick) {
      this.entities = entities;
      this.lastScanTick = lastScanTick;
    }
  }

  private static final class PredatorChunkKey {
    private final UUID worldId;
    private final int chunkX;
    private final int chunkZ;
    private final int predatorSignature;

    private PredatorChunkKey(UUID worldId, int chunkX, int chunkZ, int predatorSignature) {
      this.worldId = worldId;
      this.chunkX = chunkX;
      this.chunkZ = chunkZ;
      this.predatorSignature = predatorSignature;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof PredatorChunkKey that)) {
        return false;
      }
      return chunkX == that.chunkX
          && chunkZ == that.chunkZ
          && predatorSignature == that.predatorSignature
          && worldId.equals(that.worldId);
    }

    @Override
    public int hashCode() {
      int result = worldId.hashCode();
      result = 31 * result + chunkX;
      result = 31 * result + chunkZ;
      result = 31 * result + predatorSignature;
      return result;
    }
  }

  private final class PredatorListener implements Listener {
    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = true)
    public void onEntityBreed(EntityBreedEvent event) {
      if (settings == null || event == null) {
        return;
      }
      if (!(event.getBreeder() instanceof Player)) {
        return;
      }
      Entity entity = event.getEntity();
      if (entity == null) {
        return;
      }
      if (settings.mobConfigs.containsKey(entity.getType())) {
        event.setCancelled(true);
      }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onEntityTargetLiving(EntityTargetLivingEntityEvent event) {
      if (settings == null || event == null) {
        return;
      }
      if (!(event.getTarget() instanceof Mob prey)) {
        return;
      }
      Entity predator = event.getEntity();
      if (predator == null || !predator.isValid()) {
        return;
      }
      List<MobTypeConfig<? extends Mob>> preyConfigs =
          settings.predatorToPrey.get(predator.getType());
      if (preyConfigs == null) {
        return;
      }
      MobTypeConfig<? extends Mob> config = findPreyConfig(preyConfigs, prey.getType());
      if (config == null) {
        return;
      }
      herdManager.triggerImmediateFlee(
          prey,
          predator.getLocation(),
          config.settings,
          Bukkit.getCurrentTick());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onEntityPathfind(EntityPathfindEvent event) {
      if (settings == null || event == null) {
        return;
      }
      Entity target = event.getTargetEntity();
      if (!(target instanceof Mob prey)) {
        return;
      }
      Entity predator = event.getEntity();
      if (predator == null || !predator.isValid()) {
        return;
      }
      List<MobTypeConfig<? extends Mob>> preyConfigs =
          settings.predatorToPrey.get(predator.getType());
      if (preyConfigs == null) {
        return;
      }
      MobTypeConfig<? extends Mob> config = findPreyConfig(preyConfigs, prey.getType());
      if (config == null) {
        return;
      }
      herdManager.triggerImmediateFlee(
          prey,
          predator.getLocation(),
          config.settings,
          Bukkit.getCurrentTick());
    }

    private MobTypeConfig<? extends Mob> findPreyConfig(
        List<MobTypeConfig<? extends Mob>> preyConfigs,
        EntityType preyType) {
      for (MobTypeConfig<? extends Mob> config : preyConfigs) {
        if (config.entityType == preyType) {
          return config;
        }
      }
      return null;
    }
  }

  private static String formatEntityType(EntityType type) {
    String name = type.name().toLowerCase(Locale.ROOT).replace('_', ' ');
    if (name.isEmpty()) {
      return type.name();
    }
    return Character.toUpperCase(name.charAt(0)) + name.substring(1);
  }

  private static boolean isSurvivalPlayer(Player player) {
    return player != null && player.getGameMode() == GameMode.SURVIVAL;
  }
}
