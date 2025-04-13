package net.yuanmiranda.cmapPaper;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class CmapPaper extends JavaPlugin {
    private String dbHost;
    private int dbPort;
    private String dbName;
    private String dbUser;
    private String dbPassword;
    private File databaseConfigFile;
    private Connection connection;
    private CommandSender globalSender;
    private Set<String> trackedPlayers;
    private Map<String, Coordinates> lastKnownPlayerCoordinates;
    private BukkitRunnable logTask;
    private BukkitRunnable sendTask;
    private BukkitRunnable keepAliveTask;
    private boolean isSending = false;
    private StringBuilder bufferedOverworldCoordinates;
    private StringBuilder bufferedNetherCoordinates;
    private StringBuilder bufferedEndCoordinates;

    @Override
    public void onEnable() {
        initializeData();
        connectDb();
    }

    @Override
    public void onDisable() {
        disconnectDb();
        saveTrackedPlayers();
        stopLogging();

        if (logTask != null) logTask.cancel();
        if (sendTask != null) sendTask.cancel();
        if (keepAliveTask != null) keepAliveTask.cancel();
    }

    private void logError(String warningMsg, Exception e) {
        getLogger().warning(warningMsg);
        if (globalSender != null) globalSender.sendMessage(warningMsg);
        e.printStackTrace();
    }

    private void initializeData() {
        File dataFolder = getDataFolder();
        if (!dataFolder.exists()) {
            if (!dataFolder.mkdirs()) {
                getLogger().warning("Failed to create data folder.");
                return;
            }
        }
        loadDbConfig();
        loadTrackedPlayers();
    }

    /**
     * Saves the list of tracked players to the "trackedPlayers.txt" file.
     *
     * <p>This method overwrites the file with the current list of tracked players.
     * Each player's name is written to a new line in the file.</p>
     *
     * <p>Logs the number of players saved, or an error message if an exception occurs.</p>
     */
    private void saveTrackedPlayers() {
        File file = new File(getDataFolder(), "trackedPlayers.txt");
        try (PrintWriter writer = new PrintWriter(new FileWriter(file, false))) {
            for (String player : trackedPlayers) {
                writer.println(player);
            }
            getLogger().info(String.format("Saved tracked players to %s", file.getName()));
        } catch (Exception e) {
            logError(String.format("Error writing file: %s", file.getName()), e);
        }
    }

    private void loadTrackedPlayers() {
        trackedPlayers = new HashSet<>();
        lastKnownPlayerCoordinates = new HashMap<>();

        bufferedOverworldCoordinates = new StringBuilder();
        bufferedNetherCoordinates = new StringBuilder();
        bufferedEndCoordinates = new StringBuilder();

        File trackedPlayersFile = new File(getDataFolder(), "trackedPlayers.txt");
        try {
            if (!trackedPlayersFile.exists()) trackedPlayersFile.createNewFile();

            BufferedReader reader = new BufferedReader(new FileReader(trackedPlayersFile));
            String line;
            while ((line = reader.readLine()) != null) {
                trackedPlayers.add(line.trim());
            }
            getLogger().info(String.format("Tracked players:\n%s", String.join("\n", trackedPlayers)));
        } catch (Exception e) {
            logError(String.format("Error reading file: %s", trackedPlayersFile.getName()), e);
        }
    }

    /**
     * Serializes and saves the database configuration to the "databaseConfig.json" file.
     *
     * <p>The configuration includes the following details:</p>
     * <ul>
     *     <li>Database host</li>
     *     <li>Database port</li>
     *     <li>Database name</li>
     *     <li>Database user</li>
     *     <li>Database password</li>
     * </ul>
     *
     * <p>Uses the Jackson {@link ObjectMapper} to write the configuration as a JSON object to the file.</p>
     * <p>Logs an info message upon successful save or an error message if an exception occurs.</p>
     */
    private void saveDbConfig() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> databaseConfig = new HashMap<>();
            databaseConfig.put("DB_HOST", dbHost);
            databaseConfig.put("DB_PORT", dbPort);
            databaseConfig.put("DB_NAME", dbName);
            databaseConfig.put("DB_USER", dbUser);
            databaseConfig.put("DB_PASSWORD", dbPassword);
            mapper.writeValue(databaseConfigFile, databaseConfig);
            getLogger().info(String.format("Saved database configuration to %s", databaseConfigFile.getName()));
        } catch (Exception e) {
            logError("Error saving database configuration.", e);
        }
    }

    /**
     * Loads the database configuration from the "databaseConfig.json" file.
     *
     * <p>The configuration includes the following details:</p>
     * <ul>
     *     <li>Database host</li>
     *     <li>Database port</li>
     *     <li>Database name</li>
     *     <li>Database user</li>
     *     <li>Database password</li>
     * </ul>
     *
     * <p>Uses Jackson's {@link ObjectMapper} to deserialize the JSON file into a {@link Map},
     * and then extracts the values to assign to the respective instance variables.</p>
     *
     * <p>Logs the loaded configuration (with sensitive information like password included)
     * or logs an error message if an exception occurs during the loading process.</p>
     */
    private void loadDbConfig() {
        try {
            databaseConfigFile = new File(getDataFolder(), "databaseConfig.json");
            if (!databaseConfigFile.exists()) {
                dbHost = "";
                dbPort = 0;
                dbName = "";
                dbUser = "";
                dbPassword = "";
                getLogger().info("Database configuration file not found. Use /cmap dbconfig <host> <port> <name> <user> <password> to set the database configuration.");
                return;
            }
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> databaseConfig = mapper.readValue(databaseConfigFile, new TypeReference<>() {
            });
            dbHost = (String) databaseConfig.get("DB_HOST");
            dbPort = (int) databaseConfig.get("DB_PORT");
            dbName = (String) databaseConfig.get("DB_NAME");
            dbUser = (String) databaseConfig.get("DB_USER");
            dbPassword = (String) databaseConfig.get("DB_PASSWORD");
            getLogger().info(String.format("Loaded database configuration from %s", databaseConfigFile.getName()));
        } catch (Exception e) {
            logError("Error loading database configuration.", e);
        }
    }

    /**
     * Establishes a connection to the PostgreSQL database using the provided
     * configuration (host, port, database name, user, and password).
     *
     * <p>If the connection cannot be made or the configuration is missing, an error is logged
     * and the stack trace is printed. On success, a message confirming the successful connection
     * to the database is logged.</p>
     *
     * <p>Additionally, a keep-alive task is started that sends a simple query to the database
     * every minute to ensure the connection remains active. If the keep-alive query fails,
     * an error is logged.</p>
     */
    private void connectDb() {
        try {
            Class.forName("org.postgresql.Driver");
            String url = String.format("jdbc:postgresql://%s:%d/%s", dbHost, dbPort, dbName);
            connection = DriverManager.getConnection(url, dbUser, dbPassword);
            getLogger().info("Connected to the database.");
        } catch (Exception e) {
            logError("Error connecting to the database.", e);
            return;
        }

        keepAliveTask = new BukkitRunnable() {
            @Override
            public void run() {
                try (PreparedStatement statement = connection.prepareStatement("SELECT 1")) {
                    statement.executeQuery();
                } catch (SQLException e) {
                    logError("Error keep-alive connection to the database.", e);
                }
            }
        };
        keepAliveTask.runTaskTimer(this, 0, 20 * 60);
        getLogger().info("Started keep-alive connection to the database.");
    }

    private void disconnectDb() {
        try {
            if (connection != null && !connection.isClosed()) connection.close();
            getLogger().info("Disconnected from the database.");
        } catch (Exception e) {
            logError("Error disconnecting from the database.", e);
        }
    }

    private void addPlayer(String player) {
        if (trackedPlayers.add(player)) {
            getLogger().info(String.format("Added %s to tracked players.", player));
            lastKnownPlayerCoordinates.put(player, new Coordinates(0, 0, ""));
        }
    }

    private void removePlayer(String player) {
        if (trackedPlayers.remove(player)) {
            getLogger().info(String.format("Removed %s from tracked players.", player));
            lastKnownPlayerCoordinates.remove(player);
        }
    }

    private boolean isPlayerOnline(String playerName) {
        return Bukkit.getPlayer(playerName) != null;
    }

    /**
     * Starts logging player coordinates and periodically sends them to the database.
     *
     * <p>This method schedules two tasks:</p>
     * <ol>
     *     <li><strong>Log Task:</strong> Logs player coordinates every tick (runs every 1 tick).</li>
     *     <li><strong>Send Task:</strong> Periodically checks and sends buffered coordinates to the database
     *         every 2 seconds (runs every 40 ticks). It ensures that sending the data occurs
     *         asynchronously to avoid blocking the main thread.</li>
     * </ol>
     *
     * <p>The method ensures that tasks are not started multiple times if they are already running.</p>
     */
    private void startLogging() {
        if (logTask != null && !logTask.isCancelled()) return;
        startLogTask();
        startSendTask();
    }

    private void startLogTask() {
        logTask = new BukkitRunnable() {
            @Override
            public void run() {
                trackPlayerCoordinates();
            }
        };

        logTask.runTaskTimer(this, 0, 1);
        getLogger().info("Started logging player coordinates.");
    }

    private void startSendTask() {
        // sendTask runs in the main thread, while the inner task runs asynchronously
        sendTask = new BukkitRunnable() {
            @Override
            public void run() {
                if (isSending) return;
                if (!bufferedOverworldCoordinates.isEmpty() || !bufferedNetherCoordinates.isEmpty() || !bufferedEndCoordinates.isEmpty()) {
                    isSending = true;
                    new BukkitRunnable() {
                        @Override
                        public void run() {
                            try {
                                sendCoordinatesToDatabase();
                            } catch (SQLException e) {
                                logError("Error sending player coordinates to the database.", e);
                            } finally {
                                isSending = false;
                            }
                        }
                    }.runTaskAsynchronously(CmapPaper.this);
                }
            }
        };

        // 20 ticks = 1 second
        sendTask.runTaskTimer(this, 0, 20 * 2);
        getLogger().info("Started sending player coordinates to the database.");
    }

    /**
     * Stops logging player coordinates and sending them to the database.
     *
     * <p>This method cancels both the logging task and the sending task:</p>
     * <ol>
     *     <li><strong>Log Task:</strong> Stops the task that logs player coordinates.</li>
     *     <li><strong>Send Task:</strong> Stops the task that periodically sends buffered coordinates to the database.</li>
     * </ol>
     *
     * <p>Logs a message when each task is successfully stopped.</p>
     */
    private void stopLogging() {
        if (logTask != null) {
            logTask.cancel();
            getLogger().info("Stopped logging player coordinates.");
        }
        if (sendTask != null) {
            sendTask.cancel();
            getLogger().info("Stopped sending player coordinates to the database.");
        }
    }

    /**
     * Tracks the coordinates of the currently tracked players.
     *
     * <p>This method checks each tracked player’s current location and compares it to their
     * last known coordinates. If the player has moved, their new coordinates are stored
     * and categorized by their dimension (Overworld, Nether, End).</p>
     *
     * <p>Coordinates for each dimension are stored in separate buffers:</p>
     * <ul>
     *     <li><strong>Overworld coordinates</strong> are stored in <code>bufferedOverworldCoordinates</code></li>
     *     <li><strong>Nether coordinates</strong> are stored in <code>bufferedNetherCoordinates</code></li>
     *     <li><strong>End coordinates</strong> are stored in <code>bufferedEndCoordinates</code></li>
     * </ul>
     *
     * <p>The method avoids unnecessary computation by early returning if there are no tracked players.</p>
     * <p>It also handles exceptions during the tracking process and logs any errors encountered.</p>
     */
    private void trackPlayerCoordinates() {
        // early return if there are no tracked players to avoid unnecessary computation
        if (trackedPlayers.isEmpty()) return;

        StringBuilder overworldCoordinates = new StringBuilder();
        StringBuilder netherCoordinates = new StringBuilder();
        StringBuilder endCoordinates = new StringBuilder();

        for (String playerName : trackedPlayers) {
            try {
                // check if the player is online
                if (!isPlayerOnline(playerName)) continue;
                Player player = Bukkit.getPlayer(playerName);
                if (player == null) continue;

                // get player's location
                int x = (int) player.getLocation().getX();
                int z = (int) player.getLocation().getZ();
                String dimension = player.getWorld().getName();
                Coordinates lastCoordinates = lastKnownPlayerCoordinates.get(playerName);

                // check if the player has moved
                if (lastCoordinates == null || x != lastCoordinates.x || z != lastCoordinates.z) {
                    lastKnownPlayerCoordinates.put(playerName, new Coordinates(x, z, dimension));

                    // get the dimension name
                    if (dimension.contains("nether")) dimension = "nether";
                    else if (dimension.contains("end")) dimension = "the_end";
                    else dimension = "overworld";

                    // append coordinates to the corresponding dimension
                    if (dimension.equals("nether")) netherCoordinates.append(x).append(", ").append(z).append("\n");
                    else if (dimension.equals("the_end")) endCoordinates.append(x).append(", ").append(z).append("\n");
                    else overworldCoordinates.append(x).append(", ").append(z).append("\n");
                }
            } catch (Exception e) {
                logError(String.format("Error tracking coordinates for player %s", playerName), e);
            }
        }

        if (!overworldCoordinates.isEmpty()) bufferedOverworldCoordinates.append(overworldCoordinates);
        if (!netherCoordinates.isEmpty()) bufferedNetherCoordinates.append(netherCoordinates);
        if (!endCoordinates.isEmpty()) bufferedEndCoordinates.append(endCoordinates);
    }

    /**
     * Sends buffered coordinates to the database.
     *
     * <p>The method checks if there are any buffered coordinates for each dimension
     * (Overworld, Nether, and End). If coordinates are available for a dimension,
     * they are inserted into the database using the <code>insertCoordinatesToDatabase</code> method.
     * After the coordinates are sent, the corresponding buffer is cleared.</p>
     *
     * <p>This method ensures that buffered coordinates are sent in batches for each dimension.</p>
     *
     * @throws SQLException if an error occurs while interacting with the database
     */
    private void sendCoordinatesToDatabase() throws SQLException {
        if (!bufferedOverworldCoordinates.isEmpty()) {
            insertCoordinatesToDatabase("overworld", bufferedOverworldCoordinates);
            bufferedOverworldCoordinates.setLength(0);
        }
        if (!bufferedNetherCoordinates.isEmpty()) {
            insertCoordinatesToDatabase("nether", bufferedNetherCoordinates);
            bufferedNetherCoordinates.setLength(0);
        }
        if (!bufferedEndCoordinates.isEmpty()) {
            insertCoordinatesToDatabase("the_end", bufferedEndCoordinates);
            bufferedEndCoordinates.setLength(0);
        }
    }

    /**
     * Inserts buffered coordinates into the database for a specific dimension.
     *
     * <p>The method constructs an SQL query to insert coordinates into the appropriate table
     * based on the given dimension (Overworld, Nether, or End). The coordinates are parsed
     * from the buffered <code>StringBuilder</code> and added to the SQL query.</p>
     *
     * <p>The SQL query inserts the coordinates in batches, where each batch corresponds to a
     * specific dimension (Overworld, Nether, or End). The <code>x</code> and <code>z</code> coordinates
     * are extracted from the buffered coordinates, which are expected to be in the format: <code>"x, z"</code>
     * per line.</p>
     *
     * <p>After building the query, the method executes it using a <code>PreparedStatement</code> to insert the
     * values into the respective dimension table in the PostgreSQL database.</p>
     *
     * @param dimension           The name of the dimension (e.g., "overworld", "nether", or "the_end").
     * @param bufferedCoordinates The coordinates to be inserted, stored as a string in the format <code>"x, z"</code>.
     * @throws SQLException If an error occurs while executing the SQL query.
     */
    private void insertCoordinatesToDatabase(String dimension, StringBuilder bufferedCoordinates) throws SQLException {
        // PostgreSQL database schema
        // tables: overworld, nether, end
        // columns: id=serial, x=int, z=int
        String query = "INSERT INTO " + dimension + " (x, z) VALUES ";
        StringBuilder values = new StringBuilder();

        String[] coordinates = bufferedCoordinates.toString().split("\n");
        for (String coordinate : coordinates) {
            String[] parts = coordinate.split(", ");
            int x = Integer.parseInt(parts[0]);
            int z = Integer.parseInt(parts[1]);
            values.append("(").append(x).append(", ").append(z).append("), ");
        }

        // remove the last comma and space
        values.setLength(values.length() - 2);

        try (PreparedStatement statement = connection.prepareStatement(query + values)) {
            statement.executeUpdate();
        }
    }

    @Override
    public boolean onCommand(@NotNull CommandSender sender, Command command, @NotNull String label, String[] args) {
        if (command.getName().equalsIgnoreCase("cmap")) {
            int argsLength = args.length;
            if (argsLength < 1) {
                sender.sendMessage("Invalid subcommand.\nUsage:\n\t/cmap add <player=sender>\n\t/cmap remove <player=sender>\n\t/cmap list\n\t/cmap start\n\t/cmap stop\n\t/cmap reload\n\t/cmap dbconfig <host> <port> <name> <user> <password>");
                return true;
            }
            String subCommand = args[0];
            String player;
            switch (subCommand) {
                case "add":
                    if (argsLength > 2) {
                        sender.sendMessage("Invalid subcommand.\nUsage:\n\t/cmap add <player>");
                        return true;
                    }
                    if (argsLength == 1) {
                        if (!(sender instanceof Player)) {
                            sender.sendMessage("Sender is not a player.\nUsage:\n\t/cmap add <player>");
                            return true;
                        }
                        player = sender.getName();
                    } else player = args[1];

                    if (isPlayerOnline(player)) {
                        addPlayer(player);
                        sender.sendMessage(String.format("Added %s to tracked players.", player));
                    } else sender.sendMessage(String.format("Player %s not found.", player));
                    break;
                case "remove":
                    if (argsLength > 2) {
                        sender.sendMessage("Invalid subcommand.\nUsage:\n\t/cmap remove <player>");
                        return true;
                    }
                    if (argsLength == 1) {
                        if (!(sender instanceof Player)) {
                            sender.sendMessage("Sender is not a player.\nUsage:\n\t/cmap remove <player>");
                            return true;
                        }
                        player = sender.getName();
                    } else player = args[1];

                    if (isPlayerOnline(player)) {
                        removePlayer(player);
                        sender.sendMessage(String.format("Removed %s from tracked players.", player));
                    } else sender.sendMessage(String.format("Player %s not found.", player));
                    break;
                case "list":
                    if (argsLength > 1) {
                        sender.sendMessage("Invalid subcommand.\nUsage:\n\t/cmap list");
                        return true;
                    }
                    String players = trackedPlayers.isEmpty() ? "No tracked players." : String.format("Tracked players:\n%s", String.join("\n", trackedPlayers));
                    sender.sendMessage(players);
                    break;
                case "start":
                    if (connection == null) {
                        sender.sendMessage("Error connecting to the database, or database configuration not found. " +
                                "Use /cmap dbconfig <host> <port> <name> <user> <password> to set the database configuration.");
                        return true;
                    }
                    if (argsLength > 1) {
                        sender.sendMessage("Invalid subcommand.\nUsage:\n\t/cmap start");
                        return true;
                    }
                    if (trackedPlayers.isEmpty()) {
                        sender.sendMessage("No tracked players.");
                        return true;
                    }
                    if (logTask != null && !logTask.isCancelled()) {
                        sender.sendMessage("Logging player coordinates is already running.");
                        return true;
                    }
                    startLogging();
                    sender.sendMessage("Started logging player coordinates.");
                    break;
                case "stop":
                    if (argsLength > 1) {
                        sender.sendMessage("Invalid subcommand.\nUsage:\n\t/cmap stop");
                        return true;
                    }
                    if (logTask == null || logTask.isCancelled()) {
                        sender.sendMessage("Logging player coordinates is already stopped.");
                        return true;
                    }
                    stopLogging();
                    sender.sendMessage("Stopped logging player coordinates.");
                    break;
                case "dbconfig":
                    if (argsLength < 6) {
                        sender.sendMessage("Invalid subcommand.\nUsage:\n\t/cmap dbconfig <host> <port> <name> <user> <password>");
                        return true;
                    }

                    try {
                        dbHost = args[1];
                        dbPort = Integer.parseInt(args[2]);
                        dbName = args[3];
                        dbUser = args[4];
                        dbPassword = args[5];
                    } catch (NumberFormatException e) {
                        sender.sendMessage("Invalid port number.");
                        return true;
                    } catch (Exception e) {
                        sender.sendMessage("Invalid database configuration.");
                        return true;
                    }
                    saveDbConfig();
                    break;
                case "reload":
                    if (argsLength > 1) {
                        sender.sendMessage("Invalid subcommand.\nUsage:\n\t/cmap reload");
                        return true;
                    }
                    onDisable();
                    onEnable();
                    sender.sendMessage("Reloaded CmapPaper.");
                    break;
                default:
                    sender.sendMessage("Invalid subcommand.\nUsage:\n\t/cmap add <player=sender>\n\t/cmap remove <player=sender>\n\t/cmap list\n\t/cmap start\n\t/cmap stop\n\t/cmap reload\n\t/cmap dbconfig <host> <port> <name> <user> <password>");
                    break;
            }
            return true;
        }
        return false;
    }

    private static class Coordinates {
        int x, z;
        String dimension;

        /**
         * @param x         x-coordinate
         * @param z         z-coordinate
         * @param dimension dimension name
         */
        Coordinates(int x, int z, String dimension) {
            this.x = x;
            this.z = z;
            this.dimension = dimension;
        }
    }
}
