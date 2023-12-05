package server.faulttolerance;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONObject;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * This class should implement your {@link Replicable} database app if you wish
 * to use Gigapaxos.
 * <p>
 * Make sure that both a single instance of Cassandra is running at the default
 * port on localhost before testing.
 * <p>
 * Tips:
 * <p>
 * 1) No server-server communication is permitted or necessary as you are using
 * gigapaxos for all that.
 * <p>
 * 2) A {@link Replicable} must be agnostic to "myID" as it is a standalone
 * replication-agnostic application that via its {@link Replicable} interface is
 * being replicated by gigapaxos. However, in this assignment, we need myID as
 * each replica uses a different keyspace (because we are pretending different
 * replicas are like different keyspaces), so we use myID only for initiating
 * the connection to the backend data store.
 * <p>
 * 3) This class is never instantiated via a main method. You can have a main
 * method for your own testing purposes but it won't be invoked by any of
 * Grader's tests.
 */
public class MyDBReplicableAppGP implements Replicable {

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 * Faster
	 * is not necessarily better, so don't sweat speed. Focus on safety.
	 */
	public static final int SLEEP = 1000;

	/**
	 * All Gigapaxos apps must either support a no-args constructor or a
	 * constructor taking a String[] as the only argument. Gigapaxos relies on
	 * adherence to this policy in order to be able to reflectively construct
	 * customer application instances.
	 *
	 * @param args Singleton array whose args[0] specifies the keyspace in the
	 *             backend data store to which this server must connect.
	 *             Optional args[1] and args[2]
	 * @throws IOException
	 */
	Cluster cluster;
	Session session;
	String keyspace;

	public MyDBReplicableAppGP(String[] args) throws IOException {
		// TODO: setup connection to the data store and keyspace
		this.keyspace = args[0];
		this.cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		this.session = cluster.connect(keyspace);
		System.out.println(System.getProperty("GIGAPAXOS_DATA_DIR"));
	}

	/**
	 * Refer documentation of {@link Replicable#execute(Request, boolean)} to
	 * understand what the boolean flag means.
	 * <p>
	 * You can assume that all requests will be of type {@link
	 * edu.umass.cs.gigapaxos.paxospackets.RequestPacket}.
	 *
	 * @param request
	 * @param b
	 * @return
	 */
	@Override
	public boolean execute(Request request, boolean b) {
		// TODO: submit request to data store
		try {
			System.out.println("execute() started");
			String queryJson = request.toString();
			JSONObject json = new JSONObject(queryJson);
			String query = json.getString("QV");
			session.execute(query);
			System.out.println("execute() ended");
			return true;
		} catch (Exception e) {
			System.out.println("Error executing request");
			return false;
		}

	}

	/**
	 * Refer documentation of
	 * {@link edu.umass.cs.gigapaxos.interfaces.Application#execute(Request)}
	 *
	 * @param request
	 * @return
	 */
	@Override
	public boolean execute(Request request) {
		try {
			System.out.println("execute() started");
			String queryJson = request.toString();
			JSONObject json = new JSONObject(queryJson);
			String query = json.getString("QV");
			session.execute(query);
			System.out.println("execute() ended");
			return true;
		} catch (Exception e) {
			System.out.println("Error executing request");
			return false;
		}

	}

	/**
	 * Refer documentation of {@link Replicable#checkpoint(String)}.
	 *
	 * @param s
	 * @return
	 */
	@Override
	public String checkpoint(String name) {
		// Create a snapshot of the Cassandra keyspace
		System.out.println("checkpoint() started");
		System.out.println("Checkpoint creating:" + name);
		String snapshotName = "snapshot_" + name;
		try {
			saveSnapshot(snapshotName);
		} catch (IOException | InterruptedException e) {
			System.out.println("Checkpoint Fail");
		}

		System.out.println("checkpoint() ended");
		return snapshotName;
	}

	public void saveSnapshot(String snapshotName) throws IOException, InterruptedException {
		// Assuming you have nodetool accessible in the system path.

		System.out.println("saveSnapshot() started");
		// String command = "nodetool snapshot -t " + snapshotName + " " + keyspace;

		// Process process = Runtime.getRuntime().exec(command);
		// int exitCode = process.waitFor();

		// if (exitCode != 0) {
		// throw new IOException("Snapshot creation failed with exit code " + exitCode);
		// }
		String tableName = "temp";
		String query = "SELECT * FROM " + keyspace + "." + tableName;
		ResultSet results = session.execute(query);

		String currentDirectory = System.getProperty("user.dir");
		System.out.println("Current working directory: " + currentDirectory);

		try (BufferedWriter writer = new BufferedWriter(new FileWriter("backup.csv"))) {
			for (Row row : results) {
				// Example: Assuming the table has two columns 'id' and 'value'
				writer.write(row.getInt("id") + "," + row.getString("value"));
				writer.newLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("Snapshot " + snapshotName + " created for keyspace " + keyspace);
		System.out.println("saveSnapshot() ended");
	}

	/**
	 * Refer documentation of {@link Replicable#restore(String, String)}
	 *
	 * @param s
	 * @param s1
	 * @return
	 */

	@Override
	public boolean restore(String name, String state) {
		System.out.println("restore() started");
		try {
			if (recoverSnapshot("backup_" + name)) {
				System.out.println("Restoring:" + name);
			}
		} catch (Exception e) {
			System.out.println("Restore Fail: " + e.getMessage());
			e.printStackTrace();
			return false;
		}

		System.out.println("restore() ended");
		return true; // Restoration successful
	}

	private boolean recoverSnapshot(String backupName) throws IOException {
		System.out.println("recoverSnapshot() started");
		String backupFilePath = System.getProperty("user.dir") + "/backup.csv";

		// Check if the backup file exists
		File backupFile = new File(backupFilePath);
		if (!backupFile.exists()) {
			System.out.println("Backup file does not exist: " + backupFilePath);
			return false;
		}

		// Read data from the backup file and insert it into the table
		try (BufferedReader reader = new BufferedReader(new FileReader(backupFile))) {
			String line;
			while ((line = reader.readLine()) != null) {
				// Assuming the backup CSV is in the format: id,value,...
				// You need to tailor this to your table's schema
				String[] values = line.split(",");
				String tableName = "temp";
				String insertCQL = "INSERT INTO " + keyspace + "." + tableName
						+ " (id, value) VALUES (" + values[0] + ", '" + values[1] + "')";
				session.execute(insertCQL);
			}
		} catch (Exception e) {
			System.out.println("Error during restoration: " + e.getMessage());
			e.printStackTrace();
			return false;
		}

		System.out.println("Restored keyspace " + keyspace + " from backup " + backupName);
		System.out.println("recoverSnapshot() ended");
		return true;
	}

	/**
	 * No request types other than {@link edu.umass.cs.gigapaxos.paxospackets
	 * .RequestPacket will be used by Grader, so you don't need to implement
	 * this method.}
	 *
	 * @param s
	 * @return
	 * @throws RequestParseException
	 */
	@Override
	public Request getRequest(String s) throws RequestParseException {
		return null;
	}

	/**
	 * @return Return all integer packet types used by this application. For an
	 *         example of how to define your own IntegerPacketType enum, refer
	 *         {@link
	 *         edu.umass.cs.reconfiguration.examples.AppRequest}. This method does
	 *         not
	 *         need to be implemented because the assignment Grader will only use
	 *         {@link
	 *         edu.umass.cs.gigapaxos.paxospackets.RequestPacket} packets.
	 */
	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return new HashSet<IntegerPacketType>();
	}
}
