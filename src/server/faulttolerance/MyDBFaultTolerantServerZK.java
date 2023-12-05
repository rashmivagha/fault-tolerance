package server.faulttolerance;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONException;
import org.json.JSONObject;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import client.AVDBClient;
import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.ReplicatedServer;

/**
 * This class should implement your replicated fault-tolerant database server if
 * you wish to use Zookeeper or other custom consensus protocols to order client
 * requests.
 * <p>
 * Refer to {@link server.ReplicatedServer} for a starting point for how to do
 * server-server messaging or to {@link server.AVDBReplicatedServer} for a
 * non-fault-tolerant replicated server.
 * <p>
 * You can assume that a single *fault-tolerant* Zookeeper server at the default
 * host:port of localhost:2181 and you can use this service as you please in the
 * implementation of this class.
 * <p>
 * Make sure that both a single instance of Cassandra and a single Zookeeper
 * server are running on their default ports before testing.
 * <p>
 * You can not store in-memory information about request logs for more than
 * {@link #MAX_LOG_SIZE} requests.
 */
public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer {

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 */
	public static final int SLEEP = 1000;

	/**
	 * Set this to true if you want all tables drpped at the end of each run
	 * of tests by GraderFaultTolerance.
	 */
	public static final boolean DROP_TABLES_AFTER_TESTS = true;

	/**
	 * Maximum permitted size of any collection that is used to maintain
	 * request-specific state, i.e., you can not maintain state for more than
	 * MAX_LOG_SIZE requests (in memory or on disk). This constraint exists to
	 * ensure that your logs don't grow unbounded, which forces
	 * checkpointing to
	 * be implemented.
	 */
	public static final int MAX_LOG_SIZE = 400;

	public static final int DEFAULT_PORT = 2181;

	final private Session session;
	final private Cluster cluster;

	protected final String myID;
	protected final MessageNIOTransport<String, String> serverMessenger;

	protected String leader;

	// this is the message queue used to track which messages have not been sent yet
	private ConcurrentHashMap<Long, JSONObject> queue = new ConcurrentHashMap<Long, JSONObject>();
	private CopyOnWriteArrayList<String> notAcked;

	// the sequencer to track the most recent request in the queue
	private static long reqnum = 0;

	synchronized static Long incrReqNum() {
		return reqnum++;
	}

	// the sequencer to track the next request to be sent
	private static long expected = 0;

	synchronized static Long incrExpected() {
		return expected++;
	}

	String zookeeperServer = "127.0.0.1:2181";
	int sessionTimeout = 3000;
	ZooKeeper zooKeeper;

	/**
	 * @param nodeConfig Server name/address configuration information read
	 *                   from
	 *                   conf/servers.properties.
	 * @param myID       The name of the keyspace to connect to, also the name
	 *                   of the server itself. You can not connect to any other
	 *                   keyspace if using Zookeeper.
	 * @param isaDB      The socket address of the backend datastore to which
	 *                   you need to establish a session.
	 * @throws IOException
	 */
	public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID,
			InetSocketAddress isaDB) throws IOException {
		super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
				nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET), isaDB, myID);
		session = (cluster = Cluster.builder().addContactPoint("127.0.0.1")
				.build()).connect(myID);

		System.out.println("NodeConfig:" + nodeConfig.getNodeIDs());
		System.out.println("MyID: " + myID);
		System.out.println("Datastore socket addy: " + isaDB);

		/*
		 * NodeConfig:[server1, server2, server0]
		 * MyID: server0
		 * Datastore socket addy: localhost/127.0.0.1:9042
		 */

		// Initialize Zookeper
		this.zooKeeper = new ZooKeeper(zookeeperServer, sessionTimeout, null);

		for (String node : nodeConfig.getNodeIDs()) {
			this.leader = node;
			System.out.println("Cassandra Keyspaces: " + node);
			// Iterating through each of the keyspaces
			// Need to attach znode for each of the keyspace
			try {
				createKeyspaceZnode(node);
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		// Do the rest of the logic to watch at the health of keyspaces and elect leader

		this.myID = myID;

		this.serverMessenger = new MessageNIOTransport<String, String>(myID, nodeConfig,
				new AbstractBytePacketDemultiplexer() {
					@Override
					public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
						handleMessageFromServer(bytes, nioHeader);
						return true;
					}
				}, true);

		log.log(Level.INFO, "Server {0} started on {1}",
				new Object[] { this.myID, this.clientMessenger.getListeningSocketAddress() });
	}
	// Create znodes

	public void createKeyspaceZnode(String keyspaceName) throws KeeperException, InterruptedException {
		String znodePath = "/keyspaces/" + keyspaceName;
		String data = keyspaceName; // You can store additional data if needed
		this.zooKeeper.create(znodePath, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	// elect Leader
	public void electLeader(String keyspaceName, String serverName) throws KeeperException, InterruptedException {
		String znodePath = "/keyspaces/" + keyspaceName + "/leader";
		String znodePrefix = "/keyspaces/" + keyspaceName + "/server-";

		// Create an ephemeral sequential znode for this server
		String serverZnode = zooKeeper.create(znodePrefix, serverName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);

		// Get the list of all server znodes
		List<String> serverZnodes = zooKeeper.getChildren("/keyspaces/" + keyspaceName, false);

		// Determine leader based on the lowest sequential znode
		Collections.sort(serverZnodes);
		String leaderZnode = serverZnodes.get(0);

		if (serverZnode.equals(leaderZnode)) {
			// This server becomes the leader
			zooKeeper.create(znodePath, serverName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		}
	}

	// Leader watcher
	public void watchLeader(String keyspaceName) throws KeeperException, InterruptedException {
		String znodePath = "/keyspaces/" + keyspaceName + "/leader";

		// Set a watch on the leader znode
		zooKeeper.exists(znodePath, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				// Handle leader change event here
				System.out.println("Leader znode changed: " + event.getPath());
				// Trigger your logic when the leader changes
			}
		});
	}

	protected static enum Type {
		REQUEST, // a server forwards a REQUEST to the leader
		PROPOSAL, // the leader broadcast the REQUEST to all the nodes
		ACKNOWLEDGEMENT; // all the nodes send back acknowledgement to the leader
	}

	/**
	 * TODO: process bytes received from clients here.
	 */
	/**
	 * TODO: process bytes received from clients here, using ZooKeeper znodes.
	 */
	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
		String request = new String(bytes);

		log.log(Level.INFO, "{0} received client message {1} from {2}",
				new Object[] { this.myID, request, header.sndr });

		JSONObject json;
		try {
			json = new JSONObject(request);
			request = json.getString(AVDBClient.Keys.REQUEST.toString());
		} catch (JSONException e) {
			e.printStackTrace();
			return;
		}

		try {
			// Create or update the znode for this request
			String znodePath = "/requests/" + incrReqNum();
			zooKeeper.create(znodePath, request.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

			// Notify the leader or process the request if this server is the leader
			if (this.myID.equals(leader)) {
				processRequest(znodePath);
			} else {
				notifyLeader(znodePath);
			}

			String response = "[success:" + request + "]";
			json.put(AVDBClient.Keys.RESPONSE.toString(), response);
			serverMessenger.send(header.sndr, json.toString().getBytes());

		} catch (KeeperException | InterruptedException | JSONException | IOException e) {
			e.printStackTrace();
		}
	}

	private void processRequest(String znodePath) throws KeeperException, InterruptedException {
		byte[] data = zooKeeper.getData(znodePath, false, null);
		String request = new String(data);
		// Process the request here...
		// Example: session.execute(request);
	}

	private void notifyLeader(String znodePath) {
		// Notify the leader about the new request
		// You can use serverMessenger to send a message to the leader
		// Include the znodePath in the message for the leader to retrieve the request
		// data
	}

	/**
	 * TODO: process bytes received from fellow servers here.
	 */
	protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
		// deserialize the request
		JSONObject json = null;
		try {
			json = new JSONObject(new String(bytes));
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		log.log(Level.INFO, "{0} received relayed message {1} from {2}",
				new Object[] { this.myID, json, header.sndr }); // simply log

		// check the type of the request
		try {
			String type = json.getString(AVDBClient.Keys.TYPE.toString());
			if (type.equals(Type.REQUEST.toString())) {
				if (myID.equals(leader)) {

					// put the request into the queue
					Long reqId = incrReqNum();
					json.put(AVDBClient.Keys.REQNUM.toString(), reqId);
					queue.put(reqId, json);
					log.log(Level.INFO, "{0} put request {1} into the queue.",
							new Object[] { this.myID, json });

					if (isReadyToSend(expected)) {
						// retrieve the first request in the queue
						JSONObject proposal = queue.remove(expected);
						if (proposal != null) {
							proposal.put(AVDBClient.Keys.TYPE.toString(), Type.PROPOSAL.toString());
							enqueue();
							broadcastRequest(proposal);
						} else {
							log.log(Level.INFO,
									"{0} is ready to send request {1}, but the message has already been retrieved.",
									new Object[] { this.myID, expected });
						}

					}
				} else {
					log.log(Level.SEVERE, "{0} received REQUEST message from {1} which should not be here.",
							new Object[] { this.myID, header.sndr });
				}
			} else if (type.equals(Type.PROPOSAL.toString())) {

				// execute the query and send back the acknowledgement
				String query = json.getString(AVDBClient.Keys.REQUEST.toString());
				long reqId = json.getLong(AVDBClient.Keys.REQNUM.toString());

				session.execute(query);

				JSONObject response = new JSONObject().put(AVDBClient.Keys.RESPONSE.toString(), this.myID)
						.put(AVDBClient.Keys.REQNUM.toString(), reqId)
						.put(AVDBClient.Keys.TYPE.toString(), Type.ACKNOWLEDGEMENT.toString());
				serverMessenger.send(header.sndr, response.toString().getBytes());
			} else if (type.equals(Type.ACKNOWLEDGEMENT.toString())) {

				// only the leader needs to handle acknowledgement
				if (myID.equals(leader)) {
					// TODO: leader processes ack here
					String node = json.getString(AVDBClient.Keys.RESPONSE.toString());
					if (dequeue(node)) {
						// if the leader has received all acks, then prepare to send the next request
						expected++;
						if (isReadyToSend(expected)) {
							JSONObject proposal = queue.remove(expected);
							if (proposal != null) {
								proposal.put(AVDBClient.Keys.TYPE.toString(), Type.PROPOSAL.toString());
								enqueue();
								broadcastRequest(proposal);
							} else {
								log.log(Level.INFO,
										"{0} is ready to send request {1}, but the message has already been retrieved.",
										new Object[] { this.myID, expected });
							}
						}
					}
				} else {
					log.log(Level.SEVERE, "{0} received ACKNOWLEDEMENT message from {1} which should not be here.",
							new Object[] { this.myID, header.sndr });
				}
			} else {
				log.log(Level.SEVERE, "{0} received unrecongonized message from {1} which should not be here.",
						new Object[] { this.myID, header.sndr });
			}

		} catch (JSONException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	private boolean isReadyToSend(long expectedId) {
		if (queue.size() > 0 && queue.containsKey(expectedId)) {
			return true;
		}
		return false;
	}

	private void broadcastRequest(JSONObject req) {
		for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()) {
			try {
				this.serverMessenger.send(node, req.toString().getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		log.log(Level.INFO, "The leader has broadcast the request {0}", new Object[] { req });
	}

	private void enqueue() {
		notAcked = new CopyOnWriteArrayList<String>();
		for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()) {
			notAcked.add(node);
		}
	}

	private boolean dequeue(String node) {
		if (!notAcked.remove(node)) {
			log.log(Level.SEVERE, "The leader does not have the key {0} in its notAcked", new Object[] { node });
		}
		if (notAcked.size() == 0)
			return true;
		return false;
	}

	/**
	 * TODO: Gracefully close any threads or messengers you created.
	 */
	public void close() {
		super.close();
		this.serverMessenger.stop();
		session.close();
		cluster.close();
		// Close ZooKeeper client
		if (zookeeper != null) {
			try {
				zookeeper.close();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static enum CheckpointRecovery {
		CHECKPOINT, RESTORE;
	}

	/**
	 * @param args args[0] must be server.properties file and args[1] must be
	 *             myID. The server prefix in the properties file must be
	 *             ReplicatedServer.SERVER_PREFIX. Optional args[2] if
	 *             specified
	 *             will be a socket address for the backend datastore.
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		new MyDBFaultTolerantServerZK(
				NodeConfigUtils.getNodeConfigFromFile(args[0], ReplicatedServer.SERVER_PREFIX,
						ReplicatedServer.SERVER_PORT_OFFSET),
				args[1], args.length > 2 ? Util
						.getInetSocketAddressFromString(args[2]) : new InetSocketAddress("localhost", 9042));
	}

}
