import java.util.List;
import java.util.ArrayList;

public class Cluster {

    private List<RaftNode> nodes;
    private RaftNode leader;

    public Cluster() {
        this.nodes = new ArrayList<>();
        this.leader = null;
    }

    public void addNode(RaftNode node) {
        nodes.add(node);
    }

    public List<RaftNode> getNodes() {
        return nodes;
    }

    public RaftNode getNodeById(String id) {
        for (RaftNode node : nodes) {
            if (node.getId().equals(id)) {
                return node;
            }
        }
        return null;
    }

    /**
     * Sets a new leader in the cluster and notifies
     * the other nodes about the leader change
     * @param newLeader
     */
    public synchronized void setLeader(RaftNode newLeader) {
        if (newLeader == null) {
            leader = null;
            return;
        }
        if (leader != null) {
            leader.onLeaderChange(newLeader.getId());
        }
        leader = newLeader;
        for (RaftNode node : nodes) {
            if (!node.getId().equals(newLeader.getId())) {
                node.onLeaderChange(newLeader.getId());
            }
        }
    }

    public RaftNode getLeader() {
        return leader;
    }

    /**
     * Broadcasts AppendEntries requests among the cluster
     * @param request
     */
    public void broadcastAppendEntries(AppendEntriesRequest request) {
        for (RaftNode node : nodes) {
            if (!node.getId().equals(request.getLeaderId()) && !node.stopped.get()) {
                AppendEntriesResponse response = node.handleAppendEntries(request);
                RaftNode leader = getLeader();
                //Don´t call onAppendEntriesResponse if it was just a heartbeat
                if (leader != null && !request.getEntries().isEmpty()) {
                    leader.onAppendEntriesResponse(node.getId(), response, request.getPrevLogIndex()
                            + request.getEntries().size());
                }
            }
        }
    }

    /**
     * Broadcasts vote requests among the cluster
     * @param request
     */
    public synchronized void broadcastRequestVote(RequestVoteRequest request) {
        for (RaftNode node : nodes) {
            if (!node.getId().equals(request.getCandidateId()) && !node.stopped.get()) {
                try {
                    RequestVoteResponse response = node.handleRequestVote(request);
                    RaftNode candidate = getNodeById(request.getCandidateId());
                    if (candidate != null) {
                        candidate.onRequestVoteResponse(response);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
