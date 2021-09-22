package net.craftersland.ecobridge.database;

import java.sql.*;

import net.craftersland.ecobridge.Eco;
import org.bukkit.Bukkit;

public class MysqlSetup {

	private static Connection connection;
	private final Eco eco;

	public MysqlSetup(Eco eco) {
		this.eco = eco;
		connectToDatabase();
		updateTables();
		databaseMaintenanceTask();
	}

	public void connectToDatabase() {

		String host = eco.getConfigHandler().getString("database.mysql.host");
		int port = eco.getConfigHandler().getInteger("database.mysql.port");
		String database = eco.getConfigHandler().getString("database.mysql.databaseName");
		String username = eco.getConfigHandler().getString("database.mysql.user");
		String password = eco.getConfigHandler().getString("database.mysql.password");
		String ecoTable = eco.getConfigHandler().getString("database.mysql.dataTableName");

		Eco.log.info("Connecting to the database...");
		//Load Drivers
		try {
			synchronized (this) {
				// Database connection
				Class.forName("com.mysql.cj.jdbc.Driver");
				setConnection(DriverManager.getConnection("jdbc:mysql://" + host + ":"
						+ port + "/" + database, username, password));
				Statement statement = connection.createStatement();
				statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + ecoTable + "(id int(10) AUTO_INCREMENT, player_uuid varchar(50) NOT NULL UNIQUE, player_name varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL, money double(30,2) NOT NULL, sync_complete varchar(5) NOT NULL, last_seen varchar(30) NOT NULL, PRIMARY KEY(id))");

			}

		} catch (ClassNotFoundException e) {
			Eco.log.severe("Could not locate drivers for mysql! Error: " + e.getMessage());
			return;
		} catch (SQLException e) {
			Eco.log.severe("Could not connect to mysql database! Error: " + e.getMessage());
			return;
		}
		Eco.log.info("Database connection successful!");
	}
	
	public Connection getConnection() {
		checkConnection();
		return connection;
	}

	public void setConnection(Connection connection) {
		MysqlSetup.connection = connection;
	}
	
	public void checkConnection() {
		try {
			if (connection == null) {
				Eco.log.warning("Connection failed. Reconnecting...");
				reConnect();
			}
			if (!connection.isValid(3)) {
				Eco.log.warning("Connection is idle or terminated. Reconnecting...");
				reConnect();
			}
			if (connection.isClosed()) {
				Eco.log.warning("Connection is closed. Reconnecting...");
				reConnect();
			}
		} catch (Exception e) {
			Eco.log.severe("Could not reconnect to Database! Error: " + e.getMessage());
		}
	}
	
	public void reConnect() {
		try {            
            long start;
			long end;
			
		    start = System.currentTimeMillis();
		    Eco.log.info("Attempting to establish a connection to the MySQL server!");
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://" + eco.getConfigHandler().getString("database.mysql.host") + ":" + eco.getConfigHandler().getString("database.mysql.port") + "/" + eco.getConfigHandler().getString("database.mysql.databaseName"));
		    end = System.currentTimeMillis();
		    Eco.log.info("Connection to MySQL server established in " + ((end - start)) + " ms!");
		} catch (Exception e) {
			Eco.log.severe("Error re-connecting to the database! Error: " + e.getMessage());
		}
	}
	
	public void closeConnection() {
		try {
			Eco.log.info("Closing database connection...");
			connection.close();
			connection = null;
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void updateTables() {
		if (connection != null) {
			DatabaseMetaData md;
	    	ResultSet rs1 = null;
	    	PreparedStatement query1 = null;
	    	try {
	    		md = connection.getMetaData();
	    		rs1 = md.getColumns(null, null, eco.getConfigHandler().getString("database.mysql.dataTableName"), "sync_complete");
	            if (rs1.next()) {
			    	
			    } else {
			        String data = "ALTER TABLE `" + eco.getConfigHandler().getString("database.mysql.dataTableName") + "` ADD sync_complete varchar(5) NOT NULL DEFAULT 'true';";
			        query1 = connection.prepareStatement(data);
			        query1.execute();
			    }
	    	} catch (Exception e) {
	    		e.printStackTrace();
	    	} finally {
	    		try {
	    			if (query1 != null) {
	    				query1.close();
	    			}
	    			if (rs1 != null) {
	    				rs1.close();
	    			}
	    		} catch (Exception e) {
	    			e.printStackTrace();
	    		}
	    	}
		}
	}
	
	private void databaseMaintenanceTask() {
		if (eco.getConfigHandler().getBoolean("database.removeOldAccounts.enabled")) {
			Bukkit.getScheduler().runTaskLaterAsynchronously(eco, () -> {
				if (connection != null) {
					long inactivityDays = Long.parseLong(eco.getConfigHandler().getString("database.removeOldAccounts.inactivity"));
					long inactivityMils = inactivityDays * 24 * 60 * 60 * 1000;
					long currentTime = System.currentTimeMillis();
					long inactiveTime = currentTime - inactivityMils;
					Eco.log.info("Database maintenance task started...");
					PreparedStatement preparedStatement = null;
					try {
						String sql = "DELETE FROM `" + eco.getConfigHandler().getString("database.mysql.dataTableName") + "` WHERE `last_seen` < ?";
						preparedStatement = connection.prepareStatement(sql);
						preparedStatement.setString(1, String.valueOf(inactiveTime));
						preparedStatement.execute();
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						try {
							if (preparedStatement != null) {
								preparedStatement.close();
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					Eco.log.info("Database maintenance complete!");
				}
			}, 100 * 20L);
		}
	}

}
