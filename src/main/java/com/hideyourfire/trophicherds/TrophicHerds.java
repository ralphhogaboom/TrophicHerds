package com.hideyourfire.trophicherds;

import com.destroystokyo.paper.entity.Pathfinder;
import com.destroystokyo.paper.entity.ai.Goal;
import com.destroystokyo.paper.entity.ai.GoalKey;
import com.destroystokyo.paper.entity.ai.GoalType;
import com.destroystokyo.paper.entity.ai.MobGoals;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.bukkit.Bukkit;
import org.bukkit.GameMode;
import org.bukkit.HeightMap;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.NamespacedKey;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.block.BlockFace;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.entity.Chicken;
import org.bukkit.entity.EntityType;
import org.bukkit.entity.Cow;
import org.bukkit.entity.Goat;
import org.bukkit.entity.Horse;
import org.bukkit.entity.LivingEntity;
import org.bukkit.entity.Mob;
import org.bukkit.entity.Player;
import org.bukkit.entity.Sheep;
import org.bukkit.entity.Tameable;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.entity.CreatureSpawnEvent;
import org.bukkit.event.entity.CreatureSpawnEvent.SpawnReason;
import org.bukkit.event.world.ChunkLoadEvent;
import org.bukkit.event.world.WorldLoadEvent;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;
import org.bukkit.util.Vector;

public final class TrophicHerds extends JavaPlugin implements Listener {
  private static final List<MobKind<? extends Mob>> SUPPORTED_MOBS = List.of(
      new MobKind<>(EntityType.COW, Cow.class, "cow"),
      new MobKind<>(EntityType.HORSE, Horse.class, "horse"),
      new MobKind<>(EntityType.SHEEP, Sheep.class, "sheep"),
      new MobKind<>(EntityType.CHICKEN, Chicken.class, "chicken"),
      new MobKind<>(EntityType.GOAT, Goat.class, "goat"));
  private static final EnumSet<EntityType> PREDATOR_TYPES = EnumSet.of(
      EntityType.WOLF,
      EntityType.POLAR_BEAR,
      EntityType.SKELETON,
      EntityType.ZOMBIE);

  private MobGoals mobGoals;
  private Settings settings;
  private BukkitTask sweepTask;
  private BukkitTask herdTask;
  private final HerdManager herdManager = new HerdManager();
  private final EnumMap<EntityType, MobTypeConfig<? extends Mob>> mobConfigs =
      new EnumMap<>(EntityType.class);
  private final Map<UUID, Integer> drowningStartTicks = new HashMap<>();

  @Override
  public void onEnable() {
    saveDefaultConfig();
    this.settings = Settings.fromConfig(getConfig());
    this.mobGoals = Bukkit.getMobGoals();
    registerMobConfigs();

    Bukkit.getPluginManager().registerEvents(this, this);
    registerExistingMobs();
    scheduleSweep();
    scheduleHerdManagement();
  }

  @Override
  public void onDisable() {
    if (sweepTask != null) {
      sweepTask.cancel();
      sweepTask = null;
    }
    if (herdTask != null) {
      herdTask.cancel();
      herdTask = null;
    }
  }

  @EventHandler
  public void onCreatureSpawn(CreatureSpawnEvent event) {
    if (event.getEntity() instanceof Mob mob) {
      addGoals(mob);
    }
  }

  @EventHandler
  public void onChunkLoad(ChunkLoadEvent event) {
    for (org.bukkit.entity.Entity entity : event.getChunk().getEntities()) {
      if (entity instanceof Mob mob) {
        addGoals(mob);
      }
    }
  }

  @EventHandler
  public void onWorldLoad(WorldLoadEvent event) {
    registerMobsInWorld(event.getWorld());
  }

  private void registerExistingMobs() {
    for (World world : Bukkit.getWorlds()) {
      registerMobsInWorld(world);
    }
  }

  private void scheduleSweep() {
    if (settings.sweepIntervalTicks <= 0) {
      return;
    }
    sweepTask = Bukkit.getScheduler().runTaskTimer(
        this,
        this::registerExistingMobs,
        settings.sweepIntervalTicks,
        settings.sweepIntervalTicks);
  }

  private void scheduleHerdManagement() {
    herdTask = Bukkit.getScheduler().runTaskTimer(
        this,
        this::tickHerds,
        20L,
        20L);
  }

  private void tickHerds() {
    int currentTick = Bukkit.getCurrentTick();
    for (World world : Bukkit.getWorlds()) {
      for (MobTypeConfig<? extends Mob> config : mobConfigs.values()) {
        if (config.settings.herdUpdateIntervalTicks > 0) {
          herdManager.updateHerdsIfDue(world, config, currentTick, mobGoals);
        }
        if (config.settings.reproductionIntervalTicks > 0) {
          herdManager.handleReproduction(world, config, currentTick);
        }
        if (config.settings.overcrowdingIntervalTicks > 0) {
          herdManager.handleOvercrowding(world, config, currentTick);
        }
        if (config.settings.herdTotalCap > 0) {
          herdManager.handleNightCull(world, config);
        }
        if (settings.drowningDurationTicks > 0) {
          handleDrowning(world, config, currentTick);
        }
      }
    }
  }

  private void registerMobsInWorld(World world) {
    for (MobTypeConfig<? extends Mob> config : mobConfigs.values()) {
      for (Mob mob : world.getEntitiesByClass(config.entityClass)) {
        addGoals(mob, config);
      }
    }
  }

  private void registerMobConfigs() {
    mobConfigs.clear();
    for (MobKind<? extends Mob> kind : SUPPORTED_MOBS) {
      MobSettings mobSettings = settings.getMobSettings(kind.entityType);
      if (mobSettings == null) {
        continue;
      }
      registerMobConfig(kind, mobSettings);
    }
  }

  private <T extends Mob> void registerMobConfig(MobKind<T> kind, MobSettings mobSettings) {
    registerMobConfigInternal(kind, mobSettings);
  }

  private <T extends Mob> void registerMobConfigInternal(MobKind<T> kind, MobSettings mobSettings) {
    GoalKey<T> fleeKey = GoalKey.of(kind.entityClass, new NamespacedKey(this, kind.configKey + "_flee"));
    GoalKey<T> grazeKey = GoalKey.of(kind.entityClass, new NamespacedKey(this, kind.configKey + "_graze"));
    mobConfigs.put(
        kind.entityType,
        new MobTypeConfig<>(kind.entityType, kind.entityClass, fleeKey, grazeKey, mobSettings));
  }

  private void addGoals(Mob mob) {
    MobTypeConfig<? extends Mob> config = mobConfigs.get(mob.getType());
    if (config != null) {
      addGoals(mob, config);
    }
  }

  private <T extends Mob> void addGoals(Mob mob, MobTypeConfig<T> config) {
    if (!config.entityClass.isInstance(mob)) {
      return;
    }
    addGoalsInternal(config.entityClass.cast(mob), config);
  }

  private <T extends Mob> void addGoalsInternal(T mob, MobTypeConfig<T> config) {
    if (!mobGoals.hasGoal(mob, config.fleeKey)) {
      mobGoals.addGoal(
          mob,
          config.settings.fleePriority,
          new FleeGoal<>(mob, config.fleeKey, config.settings, herdManager, config.entityType));
    }
    if (config.settings.grazeIntervalTicks > 0
        && config.settings.grazeRadius > 0.0
        && config.settings.grazeSpeed > 0.0
        && !mobGoals.hasGoal(mob, config.grazeKey)) {
      mobGoals.addGoal(
          mob,
          config.settings.grazePriority,
          new GrazeGoal<>(mob, config.grazeKey, config.settings, herdManager, config.entityType));
    }
  }

  private static Player findClosestPlayer(Mob mob, double range) {
    Location location = mob.getLocation();
    Collection<Player> nearbyPlayers = mob.getWorld().getNearbyPlayers(
        location,
        range,
        player -> player.getGameMode() != GameMode.SPECTATOR && !player.isDead());
    Player closest = null;
    double closestDistanceSq = Double.MAX_VALUE;
    for (Player player : nearbyPlayers) {
      double distanceSq = player.getLocation().distanceSquared(location);
      if (distanceSq < closestDistanceSq) {
        closestDistanceSq = distanceSq;
        closest = player;
      }
    }
    return closest;
  }

  private static LivingEntity findClosestPredator(Mob mob, double range) {
    Location location = mob.getLocation();
    Collection<org.bukkit.entity.Entity> nearby = mob.getWorld().getNearbyEntities(
        location,
        range,
        range,
        range,
        entity -> entity instanceof LivingEntity
            && PREDATOR_TYPES.contains(entity.getType())
            && !entity.isDead());
    LivingEntity closest = null;
    double closestDistanceSq = Double.MAX_VALUE;
    for (org.bukkit.entity.Entity entity : nearby) {
      if (entity instanceof Tameable tameable && tameable.isTamed()) {
        continue;
      }
      double distanceSq = entity.getLocation().distanceSquared(location);
      if (distanceSq < closestDistanceSq) {
        closestDistanceSq = distanceSq;
        closest = (LivingEntity) entity;
      }
    }
    return closest;
  }

  private static LivingEntity findClosestThreat(Mob mob, double range) {
    Player player = findClosestPlayer(mob, range);
    LivingEntity predator = findClosestPredator(mob, range);
    if (player == null) {
      return predator;
    }
    if (predator == null) {
      return player;
    }
    double playerDistanceSq = player.getLocation().distanceSquared(mob.getLocation());
    double predatorDistanceSq = predator.getLocation().distanceSquared(mob.getLocation());
    return playerDistanceSq <= predatorDistanceSq ? player : predator;
  }

  private static Location findSafeGroundLocation(Location candidate, boolean avoidWater) {
    World world = candidate.getWorld();
    if (world == null) {
      return null;
    }
    Block ground = world.getHighestBlockAt(
        candidate.getBlockX(),
        candidate.getBlockZ(),
        HeightMap.MOTION_BLOCKING_NO_LEAVES);
    if (avoidWater && ground.isLiquid()) {
      return null;
    }
    if (!ground.getType().isSolid()) {
      return null;
    }
    Block head = ground.getRelative(BlockFace.UP);
    Block aboveHead = ground.getRelative(BlockFace.UP, 2);
    if (!head.isPassable() || !aboveHead.isPassable()) {
      return null;
    }
    return ground.getLocation().add(0.5, 1.0, 0.5);
  }

  private static boolean isNight(World world) {
    long time = world.getTime();
    return time >= 13000L && time <= 23000L;
  }

  private static boolean canDespawn(Mob mob) {
    if (!mob.isValid()) {
      return false;
    }
    if (mob.getCustomName() != null) {
      return false;
    }
    if (mob instanceof Tameable tameable && tameable.isTamed()) {
      return false;
    }
    return true;
  }

  private static boolean isDeepWater(Mob mob) {
    if (!mob.isInWater()) {
      return false;
    }
    Block feet = mob.getLocation().getBlock();
    Block above = feet.getRelative(BlockFace.UP);
    return isWaterBlock(feet) && isWaterBlock(above);
  }

  private static boolean isWaterBlock(Block block) {
    Material type = block.getType();
    return type == Material.WATER || type == Material.BUBBLE_COLUMN;
  }

  private void handleDrowning(World world, MobTypeConfig<? extends Mob> config, int currentTick) {
    int drownTicks = settings.drowningDurationTicks;
    for (Mob mob : world.getEntitiesByClass(config.entityClass)) {
      if (!mob.isValid()) {
        drowningStartTicks.remove(mob.getUniqueId());
        continue;
      }
      UUID id = mob.getUniqueId();
      if (!isDeepWater(mob)) {
        drowningStartTicks.remove(id);
        continue;
      }
      Integer startTick = drowningStartTicks.get(id);
      if (startTick == null) {
        drowningStartTicks.put(id, currentTick);
        continue;
      }
      if (currentTick - startTick >= drownTicks) {
        if (canDespawn(mob)) {
          mob.remove();
        }
        drowningStartTicks.remove(id);
      }
    }
  }

  private static final class Settings {
    private final EnumMap<EntityType, MobSettings> mobSettings;
    private final int sweepIntervalTicks;
    private final int drowningDurationTicks;

    private Settings(
        EnumMap<EntityType, MobSettings> mobSettings,
        int sweepIntervalTicks,
        int drowningDurationTicks) {
      this.mobSettings = mobSettings;
      this.sweepIntervalTicks = sweepIntervalTicks;
      this.drowningDurationTicks = drowningDurationTicks;
    }

    private static Settings fromConfig(FileConfiguration config) {
      EnumMap<EntityType, MobSettings> mobSettings = new EnumMap<>(EntityType.class);
      for (MobKind<? extends Mob> kind : SUPPORTED_MOBS) {
        mobSettings.put(
            kind.entityType,
            MobSettings.fromConfig(config, "mobs." + kind.configKey));
      }
      int sweepIntervalTicks = config.getInt("sweep-interval-ticks", 200);
      int drowningDurationSeconds = config.getInt("drowning-duration-seconds", 300);
      int drowningDurationTicks = Math.max(0, drowningDurationSeconds) * 20;

      return new Settings(
          mobSettings,
          Math.max(0, sweepIntervalTicks),
          drowningDurationTicks);
    }

    private MobSettings getMobSettings(EntityType type) {
      return mobSettings.get(type);
    }
  }

  private static final class MobSettings {
    private final double awarenessDistance;
    private final double fleeDistance;
    private final double panicDistance;
    private final int panicDurationTicks;
    private final double fleeSpeed;
    private final double panicSpeed;
    private final double fleeTargetDistance;
    private final double panicTargetDistance;
    private final double searchAngleRadians;
    private final int searchAttempts;
    private final int pathUpdateTicks;
    private final int maxSlope;
    private final boolean avoidWater;
    private final int fleePriority;
    private final double herdRadius;
    private final int herdTargetSize;
    private final int herdTotalCap;
    private final int reproductionIntervalTicks;
    private final int herdUpdateIntervalTicks;
    private final double herdCohesionSpeed;
    private final int reproductionAttempts;
    private final int overcrowdingIntervalTicks;
    private final FleeMode fleeMode;
    private final int grazePriority;
    private final double grazeRadius;
    private final int grazeIntervalTicks;
    private final double grazeSpeed;
    private final int grazeSearchAttempts;
    private final double nightHerdRadiusMultiplier;
    private final double nightHerdSpeedMultiplier;
    private final double threatHerdRadiusMultiplier;
    private final double threatHerdSpeedMultiplier;

    private MobSettings(
        double awarenessDistance,
        double fleeDistance,
        double panicDistance,
        int panicDurationTicks,
        double fleeSpeed,
        double panicSpeed,
        double fleeTargetDistance,
        double panicTargetDistance,
        double searchAngleRadians,
        int searchAttempts,
        int pathUpdateTicks,
        int maxSlope,
        boolean avoidWater,
        int fleePriority,
        double herdRadius,
        int herdTargetSize,
        int herdTotalCap,
        int reproductionIntervalTicks,
        int herdUpdateIntervalTicks,
        double herdCohesionSpeed,
        int reproductionAttempts,
        int overcrowdingIntervalTicks,
        FleeMode fleeMode,
        int grazePriority,
        double grazeRadius,
        int grazeIntervalTicks,
        double grazeSpeed,
        int grazeSearchAttempts,
        double nightHerdRadiusMultiplier,
        double nightHerdSpeedMultiplier,
        double threatHerdRadiusMultiplier,
        double threatHerdSpeedMultiplier) {
      this.awarenessDistance = awarenessDistance;
      this.fleeDistance = fleeDistance;
      this.panicDistance = panicDistance;
      this.panicDurationTicks = panicDurationTicks;
      this.fleeSpeed = fleeSpeed;
      this.panicSpeed = panicSpeed;
      this.fleeTargetDistance = fleeTargetDistance;
      this.panicTargetDistance = panicTargetDistance;
      this.searchAngleRadians = searchAngleRadians;
      this.searchAttempts = searchAttempts;
      this.pathUpdateTicks = pathUpdateTicks;
      this.maxSlope = maxSlope;
      this.avoidWater = avoidWater;
      this.fleePriority = fleePriority;
      this.herdRadius = herdRadius;
      this.herdTargetSize = herdTargetSize;
      this.herdTotalCap = herdTotalCap;
      this.reproductionIntervalTicks = reproductionIntervalTicks;
      this.herdUpdateIntervalTicks = herdUpdateIntervalTicks;
      this.herdCohesionSpeed = herdCohesionSpeed;
      this.reproductionAttempts = reproductionAttempts;
      this.overcrowdingIntervalTicks = overcrowdingIntervalTicks;
      this.fleeMode = fleeMode;
      this.grazePriority = grazePriority;
      this.grazeRadius = grazeRadius;
      this.grazeIntervalTicks = grazeIntervalTicks;
      this.grazeSpeed = grazeSpeed;
      this.grazeSearchAttempts = grazeSearchAttempts;
      this.nightHerdRadiusMultiplier = nightHerdRadiusMultiplier;
      this.nightHerdSpeedMultiplier = nightHerdSpeedMultiplier;
      this.threatHerdRadiusMultiplier = threatHerdRadiusMultiplier;
      this.threatHerdSpeedMultiplier = threatHerdSpeedMultiplier;
    }

    private static MobSettings fromConfig(FileConfiguration config, String basePath) {
      double awarenessDistance = config.getDouble(basePath + ".awareness-distance", 24.0);
      double fleeDistance = config.getDouble(basePath + ".flee-distance", 12.0);
      double panicDistance = config.getDouble(basePath + ".panic-distance", 6.0);
      int panicDurationSeconds = config.getInt(basePath + ".panic-duration-seconds", 10);
      double fleeSpeed = config.getDouble(basePath + ".flee-speed", 1.1);
      double panicSpeed = config.getDouble(basePath + ".panic-speed", 1.5);
      double fleeTargetDistance = config.getDouble(basePath + ".flee-target-distance", 14.0);
      double panicTargetDistance = config.getDouble(basePath + ".panic-target-distance", 24.0);
      double searchAngleDegrees = config.getDouble(basePath + ".search-angle-degrees", 65.0);
      int searchAttempts = config.getInt(basePath + ".search-attempts", 12);
      int pathUpdateTicks = config.getInt(basePath + ".path-update-ticks", 20);
      int maxSlope = config.getInt(basePath + ".max-slope", 2);
      boolean avoidWater = config.getBoolean(basePath + ".avoid-water", true);
      int fleePriority = config.getInt(basePath + ".flee-goal-priority", 2);
      double herdRadius = config.getDouble(basePath + ".herd-radius", 14.0);
      int herdTargetSize = config.getInt(basePath + ".herd-target-size", 4);
      int herdTotalCap = config.getInt(basePath + ".herd-total-cap", 24);
      int reproductionIntervalTicks = config.getInt(basePath + ".reproduction-interval-ticks", 2400);
      int herdUpdateIntervalTicks = config.getInt(basePath + ".herd-update-interval-ticks", 100);
      double herdCohesionSpeed = config.getDouble(basePath + ".herd-cohesion-speed", 1.0);
      int reproductionAttempts = config.getInt(basePath + ".reproduction-attempts", 8);
      int overcrowdingIntervalTicks = config.getInt(basePath + ".overcrowding-interval-ticks", 200);
      String fleeModeValue = config.getString(basePath + ".flee-mode", "scatter");
      int grazePriority = config.getInt(basePath + ".graze-goal-priority", 4);
      double grazeRadius = config.getDouble(basePath + ".graze-radius", 12.0);
      int grazeIntervalTicks = config.getInt(basePath + ".graze-interval-ticks", 200);
      double grazeSpeed = config.getDouble(basePath + ".graze-speed", 1.0);
      int grazeSearchAttempts = config.getInt(basePath + ".graze-search-attempts", 12);
      double nightHerdRadiusMultiplier = config.getDouble(basePath + ".night-herd-radius-multiplier", 0.6);
      double nightHerdSpeedMultiplier = config.getDouble(basePath + ".night-herd-speed-multiplier", 1.2);
      double threatHerdRadiusMultiplier = config.getDouble(basePath + ".threat-herd-radius-multiplier", 0.7);
      double threatHerdSpeedMultiplier = config.getDouble(basePath + ".threat-herd-speed-multiplier", 1.1);
      int panicDurationTicks = Math.max(1, panicDurationSeconds) * 20;
      FleeMode fleeMode = FleeMode.fromConfig(fleeModeValue);

      return new MobSettings(
          awarenessDistance,
          fleeDistance,
          panicDistance,
          panicDurationTicks,
          fleeSpeed,
          panicSpeed,
          fleeTargetDistance,
          panicTargetDistance,
          Math.toRadians(searchAngleDegrees),
          Math.max(1, searchAttempts),
          Math.max(5, pathUpdateTicks),
          Math.max(0, maxSlope),
          avoidWater,
          fleePriority,
          Math.max(0.0, herdRadius),
          Math.max(1, herdTargetSize),
          Math.max(0, herdTotalCap),
          Math.max(0, reproductionIntervalTicks),
          Math.max(0, herdUpdateIntervalTicks),
          Math.max(0.0, herdCohesionSpeed),
          Math.max(1, reproductionAttempts),
          Math.max(0, overcrowdingIntervalTicks),
          fleeMode,
          Math.max(0, grazePriority),
          Math.max(0.0, grazeRadius),
          Math.max(0, grazeIntervalTicks),
          Math.max(0.0, grazeSpeed),
          Math.max(1, grazeSearchAttempts),
          Math.max(0.0, nightHerdRadiusMultiplier),
          Math.max(0.0, nightHerdSpeedMultiplier),
          Math.max(0.0, threatHerdRadiusMultiplier),
          Math.max(0.0, threatHerdSpeedMultiplier));
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
    private final GoalKey<T> fleeKey;
    private final GoalKey<T> grazeKey;
    private final MobSettings settings;

    private MobTypeConfig(
        EntityType entityType,
        Class<T> entityClass,
        GoalKey<T> fleeKey,
        GoalKey<T> grazeKey,
        MobSettings settings) {
      this.entityType = entityType;
      this.entityClass = entityClass;
      this.fleeKey = fleeKey;
      this.grazeKey = grazeKey;
      this.settings = settings;
    }
  }

  private enum FleeMode {
    GROUPED,
    SCATTER;

    private static FleeMode fromConfig(String value) {
      if (value == null) {
        return SCATTER;
      }
      String normalized = value.toLowerCase(Locale.ROOT);
      if (normalized.equals("grouped") || normalized.equals("group")) {
        return GROUPED;
      }
      return SCATTER;
    }
  }

  private static final class HerdManager {
    private final Map<World, EnumMap<EntityType, HerdCache>> herdCaches = new HashMap<>();
    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    private void updateHerdsIfDue(
        World world,
        MobTypeConfig<? extends Mob> config,
        int currentTick,
        MobGoals mobGoals) {
      HerdCache cache = getCache(world, config.entityType);
      if (currentTick - cache.lastUpdateTick < config.settings.herdUpdateIntervalTicks) {
        return;
      }
      List<Mob> mobs = new ArrayList<>(world.getEntitiesByClass(config.entityClass));
      cache.herds = buildHerds(mobs, config.settings.herdRadius);
      cache.lastUpdateTick = currentTick;
      applyCohesion(cache.herds, config, mobGoals, world);
    }

    private void handleReproduction(World world, MobTypeConfig<? extends Mob> config, int currentTick) {
      HerdCache cache = getCache(world, config.entityType);
      if (currentTick - cache.lastReproductionTick < config.settings.reproductionIntervalTicks) {
        return;
      }
      List<HerdGroup> herds = cache.herds;
      if (herds == null) {
        List<Mob> mobs = new ArrayList<>(world.getEntitiesByClass(config.entityClass));
        herds = buildHerds(mobs, config.settings.herdRadius);
        cache.herds = herds;
      }
      for (HerdGroup herd : herds) {
        if (herd.members.size() >= config.settings.herdTargetSize) {
          continue;
        }
        int availableBlocks = countHerdCapacity(herd, config.settings);
        if (availableBlocks <= 0 || herd.members.size() >= availableBlocks) {
          continue;
        }
        Location spawnLocation = findHerdSpawnLocation(herd, config.settings);
        if (spawnLocation == null) {
          continue;
        }
        try {
          Mob spawned = world.spawn(
              spawnLocation,
              config.entityClass,
              SpawnReason.CUSTOM,
              true,
              entity -> {});
          if (spawned != null) {
            herd.members.add(spawned);
          }
        } catch (IllegalArgumentException ignored) {
          // Skip invalid spawn locations or entity types.
        }
      }
      cache.lastReproductionTick = currentTick;
    }

    private void handleOvercrowding(World world, MobTypeConfig<? extends Mob> config, int currentTick) {
      HerdCache cache = getCache(world, config.entityType);
      if (currentTick - cache.lastOvercrowdingTick < config.settings.overcrowdingIntervalTicks) {
        return;
      }
      List<HerdGroup> herds = cache.herds;
      if (herds == null) {
        List<Mob> mobs = new ArrayList<>(world.getEntitiesByClass(config.entityClass));
        herds = buildHerds(mobs, config.settings.herdRadius);
        cache.herds = herds;
      }
      for (HerdGroup herd : herds) {
        int availableBlocks = countHerdCapacity(herd, config.settings);
        if (herd.members.size() <= availableBlocks) {
          continue;
        }
        List<Mob> removable = new ArrayList<>();
        for (Mob member : herd.members) {
          if (member == null || !member.isValid()) {
            continue;
          }
          if (canDespawn(member)) {
            removable.add(member);
          }
        }
        while (herd.members.size() > availableBlocks && !removable.isEmpty()) {
          int index = random.nextInt(removable.size());
          Mob candidate = removable.remove(index);
          if (candidate.isValid()) {
            candidate.remove();
            herd.members.remove(candidate);
          }
        }
      }
      cache.lastOvercrowdingTick = currentTick;
    }

    private void handleNightCull(World world, MobTypeConfig<? extends Mob> config) {
      if (!isNight(world)) {
        return;
      }
      HerdCache cache = getCache(world, config.entityType);
      long day = world.getFullTime() / 24000L;
      if (cache.lastCullDay == day) {
        return;
      }
      List<Mob> mobs = new ArrayList<>(world.getEntitiesByClass(config.entityClass));
      int total = mobs.size();
      if (total <= config.settings.herdTotalCap) {
        cache.lastCullDay = day;
        return;
      }
      List<HerdGroup> herds = cache.herds;
      if (herds == null) {
        herds = buildHerds(mobs, config.settings.herdRadius);
        cache.herds = herds;
      }
      for (HerdGroup herd : herds) {
        if (total <= config.settings.herdTotalCap) {
          break;
        }
        if (herd.members.size() <= config.settings.herdTargetSize) {
          continue;
        }
        List<Mob> removable = new ArrayList<>();
        for (Mob member : herd.members) {
          if (canDespawn(member)) {
            removable.add(member);
          }
        }
        while (total > config.settings.herdTotalCap
            && herd.members.size() > config.settings.herdTargetSize
            && !removable.isEmpty()) {
          int index = random.nextInt(removable.size());
          Mob candidate = removable.remove(index);
          if (candidate.isValid()) {
            candidate.remove();
            herd.members.remove(candidate);
            total--;
          }
        }
      }
      cache.lastCullDay = day;
    }

    private HerdGroup findHerd(EntityType entityType, Mob mob) {
      HerdCache cache = getCache(mob.getWorld(), entityType);
      if (cache.herds == null) {
        return null;
      }
      for (HerdGroup herd : cache.herds) {
        if (herd.members.contains(mob)) {
          return herd;
        }
      }
      return null;
    }

    private HerdCache getCache(World world, EntityType type) {
      return herdCaches
          .computeIfAbsent(world, ignored -> new EnumMap<>(EntityType.class))
          .computeIfAbsent(type, ignored -> new HerdCache());
    }

    private List<HerdGroup> buildHerds(List<? extends Mob> mobs, double radius) {
      double radiusSq = radius * radius;
      List<HerdGroup> herds = new ArrayList<>();
      boolean[] assigned = new boolean[mobs.size()];
      for (int i = 0; i < mobs.size(); i++) {
        if (assigned[i]) {
          continue;
        }
        List<Mob> members = new ArrayList<>();
        Deque<Integer> queue = new ArrayDeque<>();
        queue.add(i);
        assigned[i] = true;
        while (!queue.isEmpty()) {
          int index = queue.removeFirst();
          Mob seed = mobs.get(index);
          if (!seed.isValid()) {
            continue;
          }
          members.add(seed);
          Location seedLocation = seed.getLocation();
          for (int j = 0; j < mobs.size(); j++) {
            if (assigned[j]) {
              continue;
            }
            Mob other = mobs.get(j);
            if (!other.isValid()) {
              assigned[j] = true;
              continue;
            }
            if (seedLocation.distanceSquared(other.getLocation()) <= radiusSq) {
              assigned[j] = true;
              queue.add(j);
            }
          }
        }
        if (!members.isEmpty()) {
          herds.add(new HerdGroup(members, calculateCenter(members)));
        }
      }
      return herds;
    }

    private void applyCohesion(
        List<HerdGroup> herds,
        MobTypeConfig<? extends Mob> config,
        MobGoals mobGoals,
        World world) {
      if (herds == null || config.settings.herdCohesionSpeed <= 0.0) {
        return;
      }
      boolean night = isNight(world);
      for (HerdGroup herd : herds) {
        if (herd.center == null) {
          continue;
        }
        boolean threatNearby = isThreatNearbyButNotFleeing(herd, config);
        double radius = config.settings.herdRadius;
        double speed = config.settings.herdCohesionSpeed;
        if (night) {
          radius *= config.settings.nightHerdRadiusMultiplier;
          speed *= config.settings.nightHerdSpeedMultiplier;
        }
        if (threatNearby) {
          radius *= config.settings.threatHerdRadiusMultiplier;
          speed *= config.settings.threatHerdSpeedMultiplier;
        }
        double radiusSq = radius * radius;
        for (Mob mob : herd.members) {
          if (!mob.isValid() || mobGoals == null) {
            continue;
          }
          if (isFleeing(mob, config, mobGoals)) {
            continue;
          }
          if (mob.getLocation().distanceSquared(herd.center) > radiusSq) {
            mob.getPathfinder().moveTo(herd.center, speed);
          }
        }
      }
    }

    private boolean isThreatNearbyButNotFleeing(HerdGroup herd, MobTypeConfig<? extends Mob> config) {
      if (herd.members.isEmpty()) {
        return false;
      }
      Mob sample = null;
      for (Mob member : herd.members) {
        if (member != null && member.isValid()) {
          sample = member;
          break;
        }
      }
      if (sample == null) {
        return false;
      }
      LivingEntity threat = findClosestThreat(sample, config.settings.awarenessDistance);
      if (threat == null) {
        return false;
      }
      double fleeDistanceSq = config.settings.fleeDistance * config.settings.fleeDistance;
      return threat.getLocation().distanceSquared(sample.getLocation()) > fleeDistanceSq;
    }

    private Location findHerdSpawnLocation(HerdGroup herd, MobSettings settings) {
      if (herd.center == null) {
        return null;
      }
      for (int i = 0; i < settings.reproductionAttempts; i++) {
        double angle = random.nextDouble(0.0, Math.PI * 2.0);
        double distance = random.nextDouble(0.0, settings.herdRadius);
        double offsetX = Math.cos(angle) * distance;
        double offsetZ = Math.sin(angle) * distance;
        Location candidate = herd.center.clone().add(offsetX, 0.0, offsetZ);
        Location safe = findSafeGroundLocation(candidate, settings.avoidWater);
        if (safe != null) {
          return safe;
        }
      }
      return null;
    }

    private boolean isFleeing(Mob mob, MobTypeConfig<? extends Mob> config, MobGoals mobGoals) {
      for (Goal<? extends Mob> goal : mobGoals.getRunningGoals(mob, GoalType.MOVE)) {
        if (goal.getKey().equals(config.fleeKey)) {
          return true;
        }
      }
      return false;
    }

    private int countHerdCapacity(HerdGroup herd, MobSettings settings) {
      if (herd == null || herd.center == null) {
        return 0;
      }
      Location center = herd.center;
      World world = center.getWorld();
      if (world == null) {
        return 0;
      }
      double radius = settings.herdRadius;
      double radiusSq = radius * radius;
      int blockRadius = (int) Math.ceil(radius);
      int centerX = center.getBlockX();
      int centerZ = center.getBlockZ();
      int available = 0;
      for (int dx = -blockRadius; dx <= blockRadius; dx++) {
        int targetX = centerX + dx;
        double dxSq = dx * dx;
        for (int dz = -blockRadius; dz <= blockRadius; dz++) {
          double distanceSq = dxSq + (dz * dz);
          if (distanceSq > radiusSq) {
            continue;
          }
          Block ground = world.getHighestBlockAt(
              targetX,
              centerZ + dz,
              HeightMap.MOTION_BLOCKING_NO_LEAVES);
          if (settings.avoidWater && ground.isLiquid()) {
            continue;
          }
          if (!ground.getType().isSolid()) {
            continue;
          }
          Block head = ground.getRelative(BlockFace.UP);
          Block aboveHead = ground.getRelative(BlockFace.UP, 2);
          if (!head.isPassable() || !aboveHead.isPassable()) {
            continue;
          }
          available++;
        }
      }
      return available;
    }

    private Location calculateCenter(List<Mob> members) {
      if (members.isEmpty()) {
        return null;
      }
      double x = 0.0;
      double y = 0.0;
      double z = 0.0;
      World world = members.get(0).getWorld();
      for (Mob mob : members) {
        Location location = mob.getLocation();
        x += location.getX();
        y += location.getY();
        z += location.getZ();
      }
      int count = members.size();
      return new Location(world, x / count, y / count, z / count);
    }
  }

  private static final class HerdCache {
    private List<HerdGroup> herds;
    private int lastUpdateTick;
    private int lastReproductionTick;
    private int lastOvercrowdingTick;
    private long lastCullDay;
  }

  private static final class HerdGroup {
    private final List<Mob> members;
    private final Location center;

    private HerdGroup(List<Mob> members, Location center) {
      this.members = members;
      this.center = center;
    }
  }

  private static final class GrazeGoal<T extends Mob> implements Goal<T> {
    private static final double ARRIVAL_DISTANCE_SQ = 2.0;

    private final T mob;
    private final GoalKey<T> key;
    private final MobSettings settings;
    private final HerdManager herdManager;
    private final EntityType entityType;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final EnumSet<GoalType> types = EnumSet.of(GoalType.MOVE);
    private Location target;
    private int nextGrazeTick;

    private GrazeGoal(
        T mob,
        GoalKey<T> key,
        MobSettings settings,
        HerdManager herdManager,
        EntityType entityType) {
      this.mob = mob;
      this.key = key;
      this.settings = settings;
      this.herdManager = herdManager;
      this.entityType = entityType;
    }

    @Override
    public boolean shouldActivate() {
      if (!mob.isValid()) {
        return false;
      }
      if (settings.grazeIntervalTicks <= 0
          || settings.grazeRadius <= 0.0
          || settings.grazeSpeed <= 0.0) {
        return false;
      }
      int currentTick = Bukkit.getCurrentTick();
      if (currentTick < nextGrazeTick) {
        return false;
      }
      target = findGrazeLocation();
      if (target == null) {
        scheduleNext(currentTick);
        return false;
      }
      return true;
    }

    @Override
    public boolean shouldStayActive() {
      if (!mob.isValid() || target == null) {
        return false;
      }
      if (mob.getPathfinder().hasPath()) {
        return true;
      }
      return target.distanceSquared(mob.getLocation()) > ARRIVAL_DISTANCE_SQ;
    }

    @Override
    public void start() {
      mob.getPathfinder().moveTo(target, settings.grazeSpeed);
    }

    @Override
    public void stop() {
      target = null;
      scheduleNext(Bukkit.getCurrentTick());
    }

    @Override
    public void tick() {
      if (target == null) {
        return;
      }
      if (!mob.getPathfinder().hasPath()
          && target.distanceSquared(mob.getLocation()) > ARRIVAL_DISTANCE_SQ) {
        mob.getPathfinder().moveTo(target, settings.grazeSpeed);
      }
    }

    @Override
    public GoalKey<T> getKey() {
      return key;
    }

    @Override
    public EnumSet<GoalType> getTypes() {
      return types;
    }

    private void scheduleNext(int currentTick) {
      int base = settings.grazeIntervalTicks;
      int jitter = base > 0 ? Math.max(1, base / 3) : 0;
      int delay = base + (jitter > 0 ? random.nextInt(jitter) : 0);
      nextGrazeTick = currentTick + delay;
    }

    private Location findGrazeLocation() {
      Location origin = mob.getLocation();
      HerdGroup herd = herdManager.findHerd(entityType, mob);
      Location herdCenter = herd != null ? herd.center : null;
      double minCenterDistance = herdCenter != null ? settings.herdRadius * 0.5 : 0.0;
      Location fallback = null;
      for (int i = 0; i < settings.grazeSearchAttempts; i++) {
        double angle = random.nextDouble(0.0, Math.PI * 2.0);
        double distance = random.nextDouble(0.0, settings.grazeRadius);
        double offsetX = Math.cos(angle) * distance;
        double offsetZ = Math.sin(angle) * distance;
        Location candidate = origin.clone().add(offsetX, 0.0, offsetZ);
        Location safe = findSafeGroundLocation(candidate, settings.avoidWater);
        if (safe == null) {
          continue;
        }
        if (herdCenter != null
            && safe.distanceSquared(herdCenter) < (minCenterDistance * minCenterDistance)) {
          fallback = safe;
          continue;
        }
        Block ground = safe.getBlock().getRelative(BlockFace.DOWN);
        if (ground.getType() == Material.GRASS_BLOCK) {
          return safe;
        }
        if (fallback == null) {
          fallback = safe;
        }
      }
      return fallback;
    }
  }

  private static final class FleeGoal<T extends Mob> implements Goal<T> {
    private static final BlockFace[] NEIGHBOR_FACES =
        new BlockFace[] {BlockFace.NORTH, BlockFace.SOUTH, BlockFace.EAST, BlockFace.WEST};

    private final T mob;
    private final GoalKey<T> key;
    private final MobSettings settings;
    private final HerdManager herdManager;
    private final EntityType entityType;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final EnumSet<GoalType> types = EnumSet.of(GoalType.MOVE);
    private Location lastThreatLocation;
    private int panicEndTick;
    private int nextPathTick;
    private boolean previousCanFloat;
    private boolean initializedCanFloat;

    private FleeGoal(
        T mob,
        GoalKey<T> key,
        MobSettings settings,
        HerdManager herdManager,
        EntityType entityType) {
      this.mob = mob;
      this.key = key;
      this.settings = settings;
      this.herdManager = herdManager;
      this.entityType = entityType;
    }

    @Override
    public boolean shouldActivate() {
      if (!mob.isValid()) {
        return false;
      }
      if (Bukkit.getCurrentTick() < panicEndTick) {
        return true;
      }
      LivingEntity threat = findClosestThreat(mob, settings.fleeDistance);
      if (threat != null) {
        lastThreatLocation = threat.getLocation();
        return true;
      }
      return false;
    }

    @Override
    public boolean shouldStayActive() {
      if (!mob.isValid()) {
        return false;
      }
      if (Bukkit.getCurrentTick() < panicEndTick) {
        return true;
      }
      LivingEntity threat = findClosestThreat(mob, settings.fleeDistance);
      if (threat != null) {
        lastThreatLocation = threat.getLocation();
        return true;
      }
      return false;
    }

    @Override
    public void start() {
      Pathfinder pathfinder = mob.getPathfinder();
      previousCanFloat = pathfinder.canFloat();
      initializedCanFloat = true;
      if (settings.avoidWater) {
        pathfinder.setCanFloat(false);
      }
    }

    @Override
    public void stop() {
      if (initializedCanFloat) {
        mob.getPathfinder().setCanFloat(previousCanFloat);
      }
      lastThreatLocation = null;
      nextPathTick = 0;
    }

    @Override
    public void tick() {
      int currentTick = Bukkit.getCurrentTick();
      LivingEntity threat = findClosestThreat(mob, settings.awarenessDistance);
      if (threat != null) {
        lastThreatLocation = threat.getLocation();
      }

      if (threat != null && threat.getLocation().distanceSquared(mob.getLocation())
          <= settings.panicDistance * settings.panicDistance) {
        panicEndTick = currentTick + settings.panicDurationTicks;
      }

      boolean panicActive = currentTick < panicEndTick;
      double speed = panicActive ? settings.panicSpeed : settings.fleeSpeed;
      double targetDistance = panicActive ? settings.panicTargetDistance : settings.fleeTargetDistance;

      if (currentTick < nextPathTick && mob.getPathfinder().hasPath()) {
        return;
      }

      Location threatLocation = lastThreatLocation;
      if (threatLocation == null && threat != null) {
        threatLocation = threat.getLocation();
      }
      if (threatLocation == null) {
        return;
      }

      Location target = findSafeFleeLocation(mob.getLocation(), threatLocation, targetDistance);
      if (target == null) {
        target = directFleeLocation(mob.getLocation(), threatLocation, targetDistance);
      }

      if (target != null) {
        mob.getPathfinder().moveTo(target, speed);
        nextPathTick = currentTick + settings.pathUpdateTicks;
      }
    }

    @Override
    public GoalKey<T> getKey() {
      return key;
    }

    @Override
    public EnumSet<GoalType> getTypes() {
      return types;
    }

    private Location directFleeLocation(Location cowLocation, Location threatLocation, double distance) {
      Vector direction = resolveBaseDirection(cowLocation, threatLocation);
      if (direction.lengthSquared() < 0.001) {
        direction = randomHorizontalVector();
      }
      direction.normalize().multiply(distance);
      Location candidate = cowLocation.clone().add(direction);
      return safeGroundLocation(candidate);
    }

    private Location findSafeFleeLocation(Location cowLocation, Location threatLocation, double distance) {
      Vector baseDirection = resolveBaseDirection(cowLocation, threatLocation);
      if (baseDirection.lengthSquared() < 0.001) {
        baseDirection = randomHorizontalVector();
      }
      baseDirection.normalize();

      Location bestLocation = null;
      int bestScore = Integer.MAX_VALUE;
      for (int i = 0; i < settings.searchAttempts; i++) {
        Vector direction = baseDirection.clone();
        if (settings.fleeMode == FleeMode.SCATTER) {
          double angle = random.nextDouble(-settings.searchAngleRadians, settings.searchAngleRadians);
          direction = rotateAroundY(direction, angle);
        }
        double distanceScale = random.nextDouble(0.7, 1.2);
        Location candidate = cowLocation.clone().add(direction.multiply(distance * distanceScale));
        Optional<LocationScore> scored = scoreLocation(candidate);
        if (scored.isEmpty()) {
          continue;
        }
        LocationScore score = scored.get();
        if (score.maxHeightDiff < bestScore) {
          bestScore = score.maxHeightDiff;
          bestLocation = score.location;
        }
      }
      return bestLocation;
    }

    private Vector resolveBaseDirection(Location mobLocation, Location threatLocation) {
      if (settings.fleeMode == FleeMode.GROUPED) {
        HerdGroup herd = herdManager.findHerd(entityType, mob);
        if (herd != null && herd.center != null) {
          return herd.center.toVector().subtract(threatLocation.toVector());
        }
      }
      return mobLocation.toVector().subtract(threatLocation.toVector());
    }

    private Optional<LocationScore> scoreLocation(Location candidate) {
      Location groundLocation = safeGroundLocation(candidate);
      if (groundLocation == null) {
        return Optional.empty();
      }
      int centerY = groundLocation.getBlockY() - 1;
      int maxDiff = 0;
      World world = groundLocation.getWorld();
      for (BlockFace face : NEIGHBOR_FACES) {
        Block neighbor = world.getHighestBlockAt(
            groundLocation.getBlockX() + face.getModX(),
            groundLocation.getBlockZ() + face.getModZ(),
            HeightMap.MOTION_BLOCKING_NO_LEAVES);
        int diff = Math.abs(centerY - neighbor.getY());
        maxDiff = Math.max(maxDiff, diff);
      }
      if (maxDiff > settings.maxSlope) {
        return Optional.empty();
      }
      return Optional.of(new LocationScore(groundLocation, maxDiff));
    }

    private Location safeGroundLocation(Location candidate) {
      return findSafeGroundLocation(candidate, settings.avoidWater);
    }

    private Vector randomHorizontalVector() {
      double angle = random.nextDouble(0.0, Math.PI * 2.0);
      return new Vector(Math.cos(angle), 0.0, Math.sin(angle));
    }

    private Vector rotateAroundY(Vector vector, double angle) {
      double cos = Math.cos(angle);
      double sin = Math.sin(angle);
      double x = vector.getX() * cos - vector.getZ() * sin;
      double z = vector.getX() * sin + vector.getZ() * cos;
      vector.setX(x);
      vector.setZ(z);
      return vector;
    }

    private static final class LocationScore {
      private final Location location;
      private final int maxHeightDiff;

      private LocationScore(Location location, int maxHeightDiff) {
        this.location = location;
        this.maxHeightDiff = maxHeightDiff;
      }
    }
  }
}
