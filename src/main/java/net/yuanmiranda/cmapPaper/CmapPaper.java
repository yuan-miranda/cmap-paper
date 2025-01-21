package net.yuanmiranda.cmapPaper;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;

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
    private static class Coordinates {
        int x, z;
        String dimension;

        /**
         * @param x x-coordinate
         * @param z z-coordinate
         * @param dimension dimension name
         */
        Coordinates(int x, int z, String dimension) {
            this.x = x;
            this.z = z;
            this.dimension = dimension;
        }
    }

    private String dbHost;
    private int dbPort;
    private String dbName;
    private String dbUser;
    private String dbPassword;
    private File databaseConfigFile;
    private Connection connection;

    // Reference to the sender of the command (for exceptions)
    private CommandSender globalSender;

    private Set<String> trackedPlayers;
    private Map<String, Coordinates> lastKnownCoordinates;

    private BukkitRunnable logTask;
    private BukkitRunnable sendTask;
    private BukkitRunnable keepAliveTask;
    private boolean isSending = false;

    private StringBuilder bufferedOverworldCoordinates;
    private StringBuilder bufferedNetherCoordinates;
    private StringBuilder bufferedEndCoordinates;

    @Override
    public void onEnable() {
        loadTrackedPlayers();
        setConnection();
    }

    @Override
    public void onDisable() {
        saveTrackedPlayers();
        stopLogging();

        try {
            if (connection != null && !connection.isClosed()) connection.close();
            getLogger().info("disconnected from the database.");
        } catch (Exception e) {
            logError("error disconnecting from the database.", e);
        }
    }

    /**
     * Logs a warning message and exception stack trace. Sends the warning
     * to the global sender if it is available.
     *
     * <p>This method logs the given warning message using the logger, sends it
     * to a global sender (if available), and prints the exception's stack trace
     * for debugging purposes.</p>
     *
     * @param warningMsg the warning message to log and send
     * @param e the exception whose stack trace is printed
     */
    private void logError(String warningMsg, Exception e) {
        getLogger().warning(warningMsg);
        if (globalSender != null) globalSender.sendMessage(warningMsg);
        e.printStackTrace();
    }

    /**
     * Loads the tracked players from a file, initializes necessary data structures,
     * and sets up the database configuration if needed.
     *
     * <p>This method performs the following tasks:</p>
     * <ul>
     *     <li>Creates the plugin data folder (if it doesn't exist).</li>
     *     <li>Ensures the "trackedPlayers.txt" file is present and reads the list of
     *         tracked players from it.</li>
     *     <li>Loads or initializes the database configuration from "databaseConfig.json".</li>
     * </ul>
     *
     * <p>Any errors encountered during file creation, database configuration loading,
     * or reading the player data file are logged using the {@link #logError} method.</p>
     *
     * @see #logError(String, Exception)
     */
    private void loadTrackedPlayers() {
        trackedPlayers = new HashSet<>();
        lastKnownCoordinates = new HashMap<>();

        bufferedOverworldCoordinates = new StringBuilder();
        bufferedNetherCoordinates = new StringBuilder();
        bufferedEndCoordinates = new StringBuilder();

        // create the data folder if it doesn't exist
        // plugin data folder: plugins/cmap-paper
        File dataFolder = getDataFolder();
        if (!dataFolder.exists()) dataFolder.mkdirs();

        File trackedPlayersFile = new File(dataFolder, "trackedPlayers.txt");
        databaseConfigFile = new File(dataFolder, "databaseConfig.json");

        // create the tracked players file if it doesn't exist
        try {
            if (!trackedPlayersFile.exists()) trackedPlayersFile.createNewFile();
        } catch (Exception e) {
            logError("error creating file: " + trackedPlayersFile.getName(), e);
            return;
        }

        // initialize or load the database configuration
        try {
            if (!databaseConfigFile.exists()) {
                dbHost = "";
                dbPort = 0;
                dbName = "";
                dbUser = "";
                dbPassword = "";
            } else loadDbConfig();
        } catch (Exception e) {
            logError("error loading database configuration.", e);
        }

        // add tracked players from the file
        try (BufferedReader reader = new BufferedReader(new FileReader(trackedPlayersFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                trackedPlayers.add(line.trim());
            }
            getLogger().info("loaded " + trackedPlayers.size() + " tracked players.");
        } catch (Exception e) {
            logError("error reading file: " + trackedPlayersFile.getName(), e);
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
            getLogger().info("saved database configuration.");
        } catch (Exception e) {
            logError("error saving database configuration.", e);
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
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> databaseConfig = mapper.readValue(databaseConfigFile, Map.class);
            dbHost = (String) databaseConfig.get("DB_HOST");
            dbPort = (int) databaseConfig.get("DB_PORT");
            dbName = (String) databaseConfig.get("DB_NAME");
            dbUser = (String) databaseConfig.get("DB_USER");
            dbPassword = (String) databaseConfig.get("DB_PASSWORD");
            getLogger().info("loaded database configuration: " +
                    dbHost + ", " + dbPort + ", " + dbName + ", " + dbUser + ", " + dbPassword);
        } catch (Exception e) {
            logError("error loading database configuration.", e);
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
    private void setConnection() {
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(
                    "jdbc:postgresql://" + dbHost + ":" + dbPort + "/" + dbName,
                    dbUser, dbPassword);
            getLogger().info("connected to the database.");
        } catch (Exception e) {
            logError("error connecting to the database: " + e.getMessage(), e);
            connection = null;
            return;
        }

        // keep-alive connection
        keepAliveTask = new BukkitRunnable() {
            @Override
            public void run() {
                try (PreparedStatement statement = connection.prepareStatement("SELECT 1")) {
                    statement.executeQuery();
                } catch (SQLException e) {
                    logError("error keep-alive connection to the database.", e);
                }
            }
        };
        keepAliveTask.runTaskTimer(this, 0, 20 * 60);
        getLogger().info("started keep-alive connection to the database.");
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
            getLogger().info("saved " + trackedPlayers.size() + " tracked players.");
        } catch (Exception e) {
            logError("error writing file: " + file.getName(), e);
        }
    }

    /**
     * Adds a player to the set of tracked players.
     *
     * <p>If the player is successfully added (i.e., the player is not already tracked),
     * logs the addition of the player to the tracked players list.</p>
     *
     * @param player the name of the player to add
     */
    private void addPlayer(String player) {
        if (trackedPlayers.add(player)) {
            getLogger().info("added " + player + " to tracked players.");
        }
    }

    /**
     * Removes a player from the set of tracked players.
     *
     * <p>If the player is successfully removed (i.e., the player was being tracked),
     * logs the removal of the player from the tracked players list.</p>
     *
     * @param player the name of the player to remove
     */
    private void removePlayer(String player) {
        if (trackedPlayers.remove(player)) {
            getLogger().info("removed " + player + " from tracked players.");
        }
    }

    /**
     * Checks if a player with the given name is currently online and valid.
     *
     * <p>A player is considered valid if they are currently online in the game.</p>
     *
     * @param playerName the name of the player to check
     * @return {@code true} if the player is online and valid, {@code false} otherwise
     */
    private boolean isValidPlayer(String playerName) {
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

        logTask = new BukkitRunnable() {
            @Override
            public void run() {
                trackPlayerCoordinates();
            }
        };
        // run every tick
        logTask.runTaskTimer(this, 0, 1);
        getLogger().info("started logging player coordinates.");

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
                                logError("error sending player coordinates to the database.", e);
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
        getLogger().info("started sending player coordinates.");
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
            getLogger().info("stopped logging player coordinates.");
        }

        if (sendTask != null) {
            sendTask.cancel();
            getLogger().info("stopped sending player coordinates.");
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
                // check if the player is valid and online
                Player player = Bukkit.getPlayer(playerName);
                if (player == null || !player.isOnline()) continue;

                // get player's location
                int x = (int) player.getLocation().getX();
                int z = (int) player.getLocation().getZ();
                String dimension = player.getWorld().getName();
                Coordinates lastCoordinates = lastKnownCoordinates.get(playerName);

                // check if the player has moved
                if (lastCoordinates == null || x != lastCoordinates.x || z != lastCoordinates.z) {
                    lastKnownCoordinates.put(playerName, new Coordinates(x, z, dimension));

                    // change dimension name to the closest match
                    if (dimension.contains("nether")) dimension = "nether";
                    else if (dimension.contains("end")) dimension = "the_end";
                    else dimension = "overworld";

                    // append coordinates to the corresponding dimension
                    if (dimension.equals("nether")) netherCoordinates.append(x).append(", ").append(z).append("\n");
                    else if (dimension.equals("the_end")) endCoordinates.append(x).append(", ").append(z).append("\n");
                    else overworldCoordinates.append(x).append(", ").append(z).append("\n");
                }
            } catch (Exception e) {
                logError("error tracking player coordinates: " + playerName, e);
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
     * @param dimension The name of the dimension (e.g., "overworld", "nether", or "the_end").
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

    /**
     * command handler
     */
    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (command.getName().equalsIgnoreCase("cmap")) {
            int argsLength = args.length;
            if (argsLength < 1) {
                sender.sendMessage("invalid subcommand.\nusage: /cmap <<add | remove> <player> | <list | start | stop | reload>>"
                                                       + "\nusage: /cmap dbconfig <host> <port> <name> <user> <password>");
                return true;
            }
            String subCommand = args[0];
            String player;
            switch (subCommand) {
                case "add":
                    if (argsLength < 2) {
                        sender.sendMessage("invalid subcommand. usage: /cmap add <player>");
                        return true;
                    }
                    player = args[1];
                    if (isValidPlayer(player)) {
                        addPlayer(player);
                        sender.sendMessage("added " + player + " to tracked players.");
                    }
                    else sender.sendMessage("player not found.");
                    break;
                case "remove":
                    if (argsLength < 2) {
                        sender.sendMessage("invalid subcommand. usage: /cmap remove <player>");
                        return true;
                    }
                    player = args[1];
                    if (isValidPlayer(player)) {
                        removePlayer(player);
                        sender.sendMessage("removed " + player + " from tracked players.");
                    }
                    else sender.sendMessage("player not found.");
                    break;
                case "list":
                    if (argsLength > 1) {
                        sender.sendMessage("invalid subcommand. usage: /cmap list");
                        return true;
                    }
                    String players = trackedPlayers.isEmpty() ? "no tracked players." : "tracked players: " + String.join(", ", trackedPlayers);
                    sender.sendMessage(players);
                    break;
                case "start":
                    if (connection == null) {
                        sender.sendMessage("error connecting to the database, or database configuration is missing. " +
                                "use /cmap dbconfig <host> <port> <name> <user> <password> to set the database configuration.");
                        return true;
                    }
                    if (argsLength > 1) {
                        sender.sendMessage("invalid subcommand. usage: /cmap start");
                        return true;
                    }
                    if (trackedPlayers.isEmpty()) {
                        sender.sendMessage("no tracked players.");
                        return true;
                    }
                    if (logTask != null && !logTask.isCancelled()) {
                        sender.sendMessage("logging player coordinates is already running.");
                        return true;
                    }
                    startLogging();
                    sender.sendMessage("started logging player coordinates.");
                    break;
                case "stop":
                    if (argsLength > 1) {
                        sender.sendMessage("invalid subcommand. usage: /cmap stop");
                        return true;
                    }
                    if (logTask == null || logTask.isCancelled()) {
                        sender.sendMessage("logging player coordinates is already stopped.");
                        return true;
                    }
                    stopLogging();
                    sender.sendMessage("stopped logging player coordinates.");
                    break;
                case "dbconfig":
                    if (argsLength < 6) {
                        sender.sendMessage("invalid subcommand. usage: /cmap dbconfig <host> <port> <name> <user> <password>");
                        return true;
                    }

                    try {
                        dbHost = args[1];
                        dbPort = Integer.parseInt(args[2]);
                        dbName = args[3];
                        dbUser = args[4];
                        dbPassword = args[5];
                    } catch (NumberFormatException e) {
                        sender.sendMessage("invalid port number.");
                        return true;
                    } catch (Exception e) {
                        sender.sendMessage("invalid database configuration.");
                        return true;
                    }
                    saveDbConfig();
                    setConnection();
                    break;
                case "reload":
                    if (argsLength > 1) {
                        sender.sendMessage("invalid subcommand. usage: /cmap reload");
                        return true;
                    }
                    onDisable();
                    onEnable();
                    sender.sendMessage("reloaded CmapPaper.");
                    break;
                default:
                    sender.sendMessage("invalid subcommand.\nusage: /cmap <<add | remove> <player> | <list | start | stop | reload>>"
                                                           + "\nusage: /cmap dbconfig <host> <port> <name> <user> <password>");
                    break;
            }
            return true;
        }
        return false;
    }
}
