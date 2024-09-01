import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test class that drives the simulation and artificially creates scenarios that are frequently occurring
 * inna cluster. By doing so, it should demonstrate the behaviour of the Raft-protocol.
 */
public class RaftClusterTest {
    private Cluster cluster;

    @Before
    public void setUp() {
        cluster = new Cluster();
        //Initialize cluster with 5 nodes
        for (int i = 1; i <= 5; i++) {
            RaftNode node = new RaftNode("node_" + i, 1000, 10000, 1000, 50, cluster);
            cluster.addNode(node);
            node.run();
        }
    }

    /**
     * Tests it a leader node is elected when at the start no leader is around
     * (initially all nodes are created as followers)
     *
     * @throws InterruptedException
     */
    @Test
    public void testLeaderElection() throws InterruptedException {
        waitForCondition(() -> cluster.getLeader() != null, 10000);
        RaftNode leader = cluster.getLeader();
        assertNotNull("Leader should be elected", leader);
        assertSame("Leader should be of type LEADER", leader.getState(), NodeState.LEADER);
    }

    /**
     * Tests the scenario of a command being submitted to the leader and committed upon
     * majority acknowledgement of the replication
     *
     * @throws InterruptedException
     */
    @Test
    public void testLogReplicationAndCommit() throws InterruptedException {
        waitForCondition(() -> cluster.getLeader() != null, 10000);
        RaftNode leader = cluster.getLeader();
        assertNotNull("Leader should be elected", leader);

        leader.submitCommand("key1=value1");
        waitForCondition(() -> cluster.getNodes().stream().allMatch(node -> node.getLog().size() == 1), 10000);
        waitForCondition(() -> cluster.getNodes().stream().allMatch(node -> node.getCommitIndex() == 0), 10000);

        for (RaftNode node : cluster.getNodes()) {
            assertEquals("Log should be replicated to all nodes", 1, node.getLog().size());
            assertEquals("Log entry should match", "key1=value1", node.getLog().getFirst().getCommand());
            assertEquals("Commit index should be updated", 0, node.getCommitIndex());
            assertEquals("State machine should be updated", "value1", node.getStateMachine().get("key1"));
        }
    }

    /**
     * Same test as above but with multiple consecutive commands
     *
     * @throws InterruptedException
     */
    @Test
    public void testMultipleCommandsReplicationAndCommit() throws InterruptedException {
        waitForCondition(() -> cluster.getLeader() != null, 10000);
        RaftNode leader = cluster.getLeader();
        assertNotNull("Leader should be elected", leader);

        leader.submitCommand("key1=value1");
        leader.submitCommand("key2=value2");
        leader.submitCommand("key3=value3");

        waitForCondition(() -> cluster.getNodes().stream().allMatch(node -> node.getLog().size() == 3), 10000);
        waitForCondition(() -> cluster.getNodes().stream().allMatch(node -> node.getCommitIndex() == 2), 10000);

        for (RaftNode node : cluster.getNodes()) {
            assertEquals("Log should contain 3 entries", 3, node.getLog().size());
            assertEquals("Commit index should be 2", 2, node.getCommitIndex());
            assertEquals("key1 should be set in state machine", "value1", node.getStateMachine().get("key1"));
            assertEquals("key2 should be set in state machine", "value2", node.getStateMachine().get("key2"));
            assertEquals("key3 should be set in state machine", "value3", node.getStateMachine().get("key3"));
        }
    }

    /**
     * Tests the clusters behavior in case of a leader timing out
     *
     * @throws InterruptedException
     */
    @Test
    public void testLeaderFailover() throws InterruptedException {
        waitForCondition(() -> cluster.getLeader() != null, 10000);
        RaftNode initialLeader = cluster.getLeader();
        assertNotNull("Initial leader should be elected", initialLeader);

        initialLeader.submitCommand("key1=value1");
        waitForCondition(() -> cluster.getNodes().stream().allMatch(node -> node.getCommitIndex() == 0), 10000);

        initialLeader.stop();
        waitForCondition(() -> cluster.getLeader() != null && !cluster.getLeader().getId().equals(initialLeader.getId()), 10000);

        RaftNode newLeader = cluster.getLeader();
        assertNotNull("New leader should be elected", newLeader);
        assertNotEquals("New leader should be different from initial leader", initialLeader.getId(), newLeader.getId());

        newLeader.submitCommand("key2=value2");
        waitForCondition(() -> cluster.getNodes().stream()
                .filter(node -> !node.getId().equals(initialLeader.getId()))
                .allMatch(node -> node.getCommitIndex() == 1), 10000);

        for (RaftNode node : cluster.getNodes()) {
            if (!node.getId().equals(initialLeader.getId())) {
                assertEquals("Log should contain 2 entries", 2, node.getLog().size());
                assertEquals("Commit index should be 1", 1, node.getCommitIndex());
                assertEquals("key1 should be set in state machine", "value1", node.getStateMachine().get("key1"));
                assertEquals("key2 should be set in state machine", "value2", node.getStateMachine().get("key2"));
            }
        }
    }

    /**
     * Helper function that allows to wait a certain time for a condition to be met
     *
     * @param condition
     * @param timeoutMillis
     * @throws InterruptedException
     */
    private void waitForCondition(java.util.function.BooleanSupplier condition, long timeoutMillis) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeoutMillis) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(50);
        }
        fail("Condition not met within timeout");
    }
}