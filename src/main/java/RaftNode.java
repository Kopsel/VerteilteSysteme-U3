import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RaftNode {

    private static final Logger logger = Logger.getLogger(RaftNode.class.getName());

    static {
        ConsoleHandler consoleHandler = new ConsoleHandler();
        consoleHandler.setFormatter(new CustomLogFormatter());
        logger.addHandler(consoleHandler);
        logger.setUseParentHandlers(false);
    }

    final AtomicBoolean stopped;
    private final String id;
    private final List<LogEntry> log;
    private final int minElectionTimeout;
    private final int maxElectionTimeout;
    private final int heartbeatInterval;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean electionInProgress;
    private final AtomicInteger votesReceived;
    private final AtomicBoolean isHeartbeatReceived;
    private final Cluster cluster;
    private final StateMachine stateMachine;
    private final Map<Integer, Map<String, Boolean>> appendResponses;
    private final Map<String, Integer> nextIndex;
    private volatile NodeState state;
    private int currentTerm;
    private volatile String votedFor;
    private int commitIndex;
    private volatile int lastApplied;
    private int electionTimeout;
    private long nextElectionTime;
    private long electionStartTime;
    private int maxElectionDuration;


    public RaftNode(String id, int minElectionTimeout, int maxElectionTimeout, int maxElectionDuration, int heartbeatInterval, Cluster cluster) {
        this.id = id;
        this.minElectionTimeout = minElectionTimeout;
        this.maxElectionTimeout = maxElectionTimeout;
        this.maxElectionDuration = maxElectionDuration;
        this.heartbeatInterval = heartbeatInterval;
        this.cluster = cluster;

        state = NodeState.FOLLOWER;
        currentTerm = 0;
        votedFor = null;
        log = new ArrayList<>();
        commitIndex = -1;
        lastApplied = -1;
        scheduler = Executors.newScheduledThreadPool(2);
        electionInProgress = new AtomicBoolean(false);
        votesReceived = new AtomicInteger(0);
        isHeartbeatReceived = new AtomicBoolean(true);
        stopped = new AtomicBoolean(false);
        appendResponses = new HashMap<>();
        stateMachine = new StateMachine();
        nextIndex = new HashMap<>();

        resetElectionTimeout();
        logger.log(Level.INFO, "{0} initialized as FOLLOWER with election timeout {1} ms", new Object[]{id, electionTimeout});
    }

    public String getId() {
        return id;
    }

    public NodeState getState() {
        return state;
    }

    public List<LogEntry> getLog() {
        return log;
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    /**
     * Handles the incoming AppendEntries requests from the leader. Several distinctions have to be made
     * based on whether there is a log conflict or the log lags behind (log inconsistencies)
     *
     * @param request
     * @return
     */
    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        //For the purpose of simulation, nodes can be manually stopped, so they become inoperable
        if (stopped.get()) {
            logger.log(Level.INFO, "{0} is stopped and will not handle AppendEntriesRequest", id);
            return new AppendEntriesResponse(currentTerm, false);
        }

        //Requests with lower term are ignored
        if (request.getTerm() < currentTerm) {
            logger.log(Level.INFO, "{0} received AppendEntriesRequest from term {1} but current term is {2}. " +
                    "Ignoring request.", new Object[]{id, request.getTerm(), currentTerm});
            return new AppendEntriesResponse(currentTerm, false);
        }

        //When new leader is established and the term is incremented, nodes become follower of that new leader (first heartbeat)
        if (request.getTerm() > currentTerm) {
            currentTerm = request.getTerm();
            state = NodeState.FOLLOWER;
            votedFor = null;
            logger.log(Level.INFO, "{0} updated term to {1} and became FOLLOWER", new Object[]{id, currentTerm});
        }

        isHeartbeatReceived.set(true);
        resetElectionTimeout();

        //Handles an incoming commit index propagation
        if (request.getEntries().isEmpty() && request.getLeaderCommit() > commitIndex) {
            commitIndex = request.getLeaderCommit();
            logger.log(Level.INFO, "{0} updated commitIndex to {1}", new Object[]{id, commitIndex});
            applyLogEntries();
            return new AppendEntriesResponse(currentTerm, true);
        }

        //Empty log entries means the request is a simple heartbeat
        if (request.getEntries().isEmpty()) {
            logger.log(Level.INFO, "{0} received heartbeat from {1} for term {2}",
                    new Object[]{id, request.getLeaderId(), request.getTerm()});
            return new AppendEntriesResponse(currentTerm, true);
        }

        //Log Inconsistency: Local log outdated
        if (request.getPrevLogIndex() >= log.size()) {
            logger.log(Level.INFO, "{0} log inconsistency detected: prevLogIndex {1} is beyond end of log",
                    new Object[]{id, request.getPrevLogIndex()});
            return new AppendEntriesResponse(currentTerm, false);
        }

        //Handles RPCs with log entries attached
        if (request.getPrevLogIndex() == -1 || (request.getPrevLogIndex() >= 0 &&
                log.get(request.getPrevLogIndex()).getTerm() == request.getPrevLogTerm())) {
            int newEntryIndex = request.getPrevLogIndex() + 1;
            for (LogEntry entry : request.getEntries()) {
                if (newEntryIndex < log.size()) {
                    if (log.get(newEntryIndex).getTerm() != entry.getTerm()) {
                        log.subList(newEntryIndex, log.size()).clear();
                        log.add(entry);
                        logger.log(Level.INFO, "{0} removed conflicting entries and appended new entry at index {1}: {2}",
                                new Object[]{id, newEntryIndex, entry});
                    }
                } else {
                    log.add(entry);
                    logger.log(Level.INFO, "{0} appended new entry at index {1}: {2}", new Object[]{id, newEntryIndex, entry});
                }
                newEntryIndex++;
            }

            if (request.getLeaderCommit() > commitIndex) {
                int oldCommitIndex = commitIndex;
                int newCommitIndex = Math.min(request.getLeaderCommit(), log.size() - 1);
                if (newCommitIndex > oldCommitIndex && newCommitIndex < log.size()) {
                    commitIndex = newCommitIndex;
                    applyLogEntries();
                    logger.log(Level.INFO, "{0} updated commitIndex to {1}", new Object[]{id, commitIndex});
                } else {
                    logger.log(Level.INFO, "{0} cannot update commitIndex to {1} as log size is {2}",
                            new Object[]{id, newCommitIndex, log.size()});
                }
            }

            return new AppendEntriesResponse(currentTerm, true);

        } else {
            logger.log(Level.INFO, "{0} log inconsistency detected: terms don't match at prevLogIndex {1}",
                    new Object[]{id, request.getPrevLogIndex()});
            return new AppendEntriesResponse(currentTerm, false);
        }
    }


    /**
     * Keeps track of the responses from followers that respond to the AppendEntries Requests. This is required in
     * order to detect when the majority of followers have successfully appended their log and the entry can be committed.
     *
     * @param logIndex
     * @param followerId
     * @param success
     */
    private synchronized void trackAppendEntriesResponse(int logIndex, String followerId, boolean success) {
        appendResponses.computeIfAbsent(logIndex, k -> new HashMap<>()).put(followerId, success);

        if (logIndex <= commitIndex) {
            return;
        }

        if (hasMajorityAcknowledged(logIndex)) {
            logger.log(Level.INFO, "{0} received append responses from the majority of nodes (self included) " +
                    "and thus committed log entries up to index {1}", new Object[]{id, commitIndex + 1});
            updateCommitIndex();
        }
    }

    private boolean hasAllEntriesUpTo(int index) {
        return log.size() > index;
    }

    private boolean hasMajorityAcknowledged(int logIndex) {
        int majority = (int) cluster.getNodes().stream().filter(node -> !node.stopped.get()).count() / 2 + 1;
        return appendResponses.getOrDefault(logIndex, new HashMap<>()).size() >= majority;
    }

    /**
     * Used to resolve log inconsistencies in case a follower lacks behind. Allows to send all missing
     * log entries to a specific follower
     *
     * @param follower
     */
    private void sendMissingEntriesToFollower(RaftNode follower) {
        int followerNextIndex = follower.getLog().size();
        List<LogEntry> entriesToSend = log.subList(followerNextIndex, log.size());

        AppendEntriesRequest request = new AppendEntriesRequest(
                currentTerm,
                id,
                followerNextIndex - 1,
                followerNextIndex > 0 ? log.get(followerNextIndex - 1).getTerm() : 0,
                entriesToSend,
                commitIndex
        );

        logger.log(Level.INFO, "{0} sending missing entries to {1}", new Object[]{id, follower.getId()});
        follower.handleAppendEntries(request);
    }

    /**
     * Propagates commit Index of the leader to its followers. If needed, a catch.up mechanism is initiated
     * that sens the missing entries to the respective follower
     */
    private void propagateCommitIndex() {
        for (RaftNode node : cluster.getNodes()) {
            if (!node.getId().equals(id) && !node.stopped.get()) {
                if (node.hasAllEntriesUpTo(commitIndex)) {
                    AppendEntriesRequest request = new AppendEntriesRequest(
                            currentTerm,
                            id,
                            log.size() - 1,
                            log.isEmpty() ? 0 : log.getLast().getTerm(),
                            new ArrayList<>(),
                            commitIndex
                    );
                    logger.log(Level.INFO, "{0} propagating commit index {1} to follower {2}",
                            new Object[]{id, commitIndex, node.getId()});
                    node.handleAppendEntries(request);
                } else {
                    logger.log(Level.INFO, "{0} not propagating commit index to {1} as it's missing entries",
                            new Object[]{id, node.getId()});
                    //Catch-Up mechanism
                    sendMissingEntriesToFollower(node);
                }
            }
        }
    }

    /**
     * Contains the logic that dictates for which type of candidates a follower will vote and generates
     * a corresponding vote response
     *
     * @param request
     * @return
     */
    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        if (stopped.get()) {
            logger.log(Level.INFO, "{0} is stopped and will not handle RequestVoteRequest", id);
            return new RequestVoteResponse(currentTerm, false, id);
        }

        //Candidate term must be greater or at least equal to won term
        if (request.getTerm() < currentTerm) {
            logger.log(Level.INFO, "{0} received RequestVoteRequest from term {1} but current term is {2}. Denying vote.",
                    new Object[]{id, request.getTerm(), currentTerm});
            return new RequestVoteResponse(currentTerm, false, id);
        }

        //If the term is higher, adjust own term
        if (request.getTerm() > currentTerm) {
            currentTerm = request.getTerm();
            votedFor = null;
            logger.log(Level.INFO, "{0} updated term to {1} due to higher term in RequestVoteRequest",
                    new Object[]{id, currentTerm});
        }

        boolean voteGranted = false;
        if ((votedFor == null || votedFor.equals(request.getCandidateId())) &&
                (request.getLastLogIndex() >= log.size() - 1) &&
                (request.getLastLogTerm() >= (log.isEmpty() ? 0 : log.getLast().getTerm()))) {
            voteGranted = true;
            votedFor = request.getCandidateId();
            logger.log(Level.INFO, "{0} granted vote to candidate {1} for term {2}",
                    new Object[]{id, request.getCandidateId(), currentTerm});
        } else {
            logger.log(Level.INFO, "{0} did not grant vote to candidate {1} for term {2}. VotedFor: {3}, LastLogIndex: {4}, LastLogTerm: {5}",
                    new Object[]{id, request.getCandidateId(), request.getTerm(), votedFor, request.getLastLogIndex(), request.getLastLogTerm()});
        }

        if (state == NodeState.FOLLOWER && voteGranted) {
            return new RequestVoteResponse(currentTerm, voteGranted, id);
        } else {
            return new RequestVoteResponse(currentTerm, false, id);
        }
    }

    /**
     * Initiates a new vote cycle and sends the corresponding vote requests to all active
     * cluster nodes
     */
    private synchronized void startElection() {
        if (electionInProgress.get()) {
            logger.log(Level.INFO, "{0} election already in progress", id);
            return;
        }

        state = NodeState.CANDIDATE;
        currentTerm++;
        votedFor = id;
        votesReceived.set(1);
        electionInProgress.set(true);
        electionStartTime = System.currentTimeMillis();

        int nonStoppedNodes = (int) cluster.getNodes().stream().filter(node -> !node.stopped.get()).count();
        int majority = nonStoppedNodes / 2 + 1;

        logger.log(Level.INFO, "{0} became CANDIDATE for term {1} and needs {2} votes to win",
                new Object[]{id, currentTerm, majority});
        logger.log(Level.INFO, "{0} voted for himself. Total votes: {1}/{2}",
                new Object[]{id, votesReceived.get(), majority});

        RequestVoteRequest requestVoteRequest = new RequestVoteRequest(
                currentTerm,
                id,
                log.size() - 1,
                log.isEmpty() ? 0 : log.getLast().getTerm());

        for (RaftNode node : cluster.getNodes()) {
            if (!node.getId().equals(id) && !node.stopped.get()) {
                logger.log(Level.INFO, "{0} sending RequestVoteRequest to {1} for term {2}",
                        new Object[]{id, node.getId(), currentTerm});
            }
        }

        cluster.broadcastRequestVote(requestVoteRequest);
        resetElectionTimeout();
    }

    /**
     * Manages the node state transition from candidate to leader and the
     * resulting consequences for the cluster
     */
    private synchronized void becomeLeader() {
        if (state != NodeState.CANDIDATE) {
            logger.log(Level.INFO, "{0} cannot become leader because it's not a candidate", id);
            return;
        }
        state = NodeState.LEADER;
        logger.log(Level.INFO, "{0} became LEADER for term {1}", new Object[]{id, currentTerm});
        scheduler.scheduleAtFixedRate(this::sendHeartbeat, 0, heartbeatInterval, TimeUnit.MILLISECONDS);
        electionInProgress.set(false);
        cluster.setLeader(this);

        for (RaftNode node : cluster.getNodes()) {
            if (!node.getId().equals(id)) {
                nextIndex.put(node.getId(), log.size());
            }
        }
    }

    private void sendHeartbeat() {
        if (stopped.get()) {
            logger.log(Level.INFO, "{0} is stopped and will not send heartbeat", id);
            return;
        }

        AppendEntriesRequest heartbeatRequest = new AppendEntriesRequest(
                currentTerm,
                id,
                log.size() - 1,
                log.isEmpty() ? 0 : log.getLast().getTerm(),
                new ArrayList<>(),
                commitIndex
        );
        logger.log(Level.INFO, "{0} sending heartbeat for term {1}", new Object[]{id, currentTerm});
        cluster.broadcastAppendEntries(heartbeatRequest);
    }

    /**
     * Method that simulates a node dropping out (going offline) in order to simulate
     * a leader timing out/ not sending out heartbeats any longer
     */
    public synchronized void stop() {
        if (stopped.get()) {
            logger.log(Level.INFO, "{0} is already stopped", id);
            return;
        }

        stopped.set(true);
        logger.log(Level.INFO, "{0} is stopping...", id);

        scheduler.shutdownNow();

        if (state == NodeState.LEADER) {
            cluster.setLeader(null);
            state = NodeState.FOLLOWER;
            logger.log(Level.INFO, "{0} was the leader and is now stopped. Leader set to null.", id);
        }
    }

    /**
     * Generates randomized election timeout in the given interval and resets the timeout
     * after a heartbeat has been received
     */
    private void resetElectionTimeout() {
        electionTimeout = ThreadLocalRandom.current().nextInt(minElectionTimeout, maxElectionTimeout + 1);
        nextElectionTime = System.currentTimeMillis() + electionTimeout;
        //logger.log(Level.INFO, "{0} reset election timeout to {1} ms", new Object[]{id, electionTimeout});
    }

    /**
     * Handles incoming votes from nodes and checks whether the majority has been reached
     *
     * @param response
     */
    public synchronized void onRequestVoteResponse(RequestVoteResponse response) {
        if (stopped.get()) {
            logger.log(Level.INFO, "{0} is stopped and will not process RequestVoteResponse", id);
            return;
        }

        logger.log(Level.INFO, "{0} received vote response from {1}: {2}",
                new Object[]{id, response.getVoterId(), response.isVoteGranted()});

        if (response.getTerm() > currentTerm) {
            currentTerm = response.getTerm();
            state = NodeState.FOLLOWER;
            votedFor = null;
            resetElectionTimeout();
            logger.log(Level.INFO, "{0} updated term to {1} and became FOLLOWER after detecting new leader",
                    new Object[]{id, currentTerm});

        } else if (response.isVoteGranted() && state == NodeState.CANDIDATE) {
            votesReceived.incrementAndGet();
            int nonStoppedNodes = (int) cluster.getNodes().stream().filter(node -> !node.stopped.get()).count();
            int majority = nonStoppedNodes / 2 + 1;

            logger.log(Level.INFO, "{0} received vote from {1}. Total votes: {2}/{3}",
                    new Object[]{id, response.getVoterId(), votesReceived.get(), majority});

            if (votesReceived.get() >= majority) {
                becomeLeader();
            }
        }
    }

    /**
     * Stops election process once a new leader has been elected
     *
     * @param newLeaderId
     */
    public synchronized void onLeaderChange(String newLeaderId) {
        if (stopped.get()) return;

        if (state == NodeState.LEADER) {
            if (!newLeaderId.equals(id)) {
                logger.log(Level.WARNING, "{0} attempted to recognize a new leader but is already a leader", id);
                return;
            }
        }

        if (state == NodeState.CANDIDATE) {
            if (newLeaderId.equals(id)) {
                logger.log(Level.WARNING, "{0} recognized itself as a new leader and should not participate in further elections", id);
                state = NodeState.LEADER;
                electionInProgress.set(false);
                return;
            }
        }

        if (state != NodeState.LEADER) {
            logger.log(Level.INFO, "{0} recognized new leader: {1}", new Object[]{id, newLeaderId});
            state = NodeState.FOLLOWER;
            resetElectionTimeout();
            electionInProgress.set(false);
        }
    }


    /**
     * Servers ast the interface for the client to submit commands to. Submitted commands are first added
     * to the leaders own log and then replicated to its followers
     *
     * @param command
     */
    public synchronized void submitCommand(String command) {
        logger.log(Level.INFO, "{0} received submit command from client", new Object[]{id});

        if (state != NodeState.LEADER) {
            logger.log(Level.WARNING, "{0} is not the leader, command cannot be submitted", id);
            return;
        }

        LogEntry newEntry = new LogEntry(currentTerm, command);
        log.add(newEntry);
        int newEntryIndex = log.size() - 1;
        logger.log(Level.INFO, "{0} appended command to log at index {1}: {2}", new Object[]{id, newEntryIndex, newEntry});

        AppendEntriesRequest request = new AppendEntriesRequest(
                currentTerm,
                id,
                newEntryIndex - 1,
                newEntryIndex > 0 ? log.get(newEntryIndex - 1).getTerm() : 0,
                List.of(newEntry),
                commitIndex
        );
        logger.log(Level.INFO, "{0} replicating command to followers", id);
        cluster.broadcastAppendEntries(request);

        trackAppendEntriesResponse(newEntryIndex, id, true); // Track the leader's own append response
    }

    /**
     * Handles incoming response to the AppendEntries request
     *
     * @param followerId
     * @param response
     * @param lastLogIndex
     */
    public void onAppendEntriesResponse(String followerId, AppendEntriesResponse response, int lastLogIndex) {
        if (state != NodeState.LEADER) return;

        if (response.isSuccess()) {
            nextIndex.put(followerId, lastLogIndex + 1);
            trackAppendEntriesResponse(lastLogIndex, followerId, true);
        } else {
            //Decrement nextIndex and retry to send again
            nextIndex.put(followerId, Math.max(0, nextIndex.get(followerId) - 1));
            sendAppendEntries(followerId);
        }
    }


    /**
     * Method to resend AppendEntries to followers whose logs are lacking behind
     *
     * @param followerId
     */
    private void sendAppendEntries(String followerId) {
        int prevLogIndex = nextIndex.get(followerId) - 1;
        int prevLogTerm = prevLogIndex >= 0 ? log.get(prevLogIndex).getTerm() : 0;
        List<LogEntry> entries = log.subList(nextIndex.get(followerId), log.size());

        AppendEntriesRequest request = new AppendEntriesRequest(
                currentTerm,
                id,
                prevLogIndex,
                prevLogTerm,
                entries,
                commitIndex
        );

        RaftNode follower = cluster.getNodeById(followerId);
        if (follower != null) {
            AppendEntriesResponse response = follower.handleAppendEntries(request);
            onAppendEntriesResponse(followerId, response, prevLogIndex + entries.size());
        }
    }


    /**
     * Updates the commit index once a majority of followers have replicated the appended log entries.
     * If this is the case, the log entries are applied to the state machine and the new commit index
     * is propagated to the other nodes
     */
    private synchronized void updateCommitIndex() {
        for (int i = commitIndex + 1; i < log.size(); i++) {
            if (log.get(i).getTerm() == currentTerm) {
                int replicationCount = 1;
                for (Map.Entry<String, Boolean> entry : appendResponses.getOrDefault(i, new HashMap<>()).entrySet()) {
                    if (entry.getValue()) replicationCount++;
                }

                int majority = (int) cluster.getNodes().stream().filter(node -> !node.stopped.get()).count() / 2 + 1;
                if (replicationCount >= majority) {
                    int oldCommitIndex = commitIndex;
                    commitIndex = i;
                    if (commitIndex > oldCommitIndex) {
                        logger.log(Level.INFO, "{0} committed log entry at index {1}: {2}",
                                new Object[]{id, commitIndex, log.get(commitIndex)});
                        applyLogEntries();
                        propagateCommitIndex();
                    }
                }
            } else {
                break;
            }
        }
    }

    private synchronized void applyLogEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = log.get(lastApplied);
            logger.log(Level.INFO, "{0} applying log entry to state machine at index {1}: {2}",
                    new Object[]{id, lastApplied, entry});
            stateMachine.apply(entry.getCommand());
        }
    }

    public void run() {
        scheduler.scheduleAtFixedRate(this::checkState, 0, 50, TimeUnit.MILLISECONDS);
    }

    /**
     * If election take too long (likely split vote scenario), the election process is reset
     */
    private synchronized void resetToFollower() {
        state = NodeState.FOLLOWER;
        votedFor = null;
        electionInProgress.set(false);
        resetElectionTimeout();
        logger.log(Level.INFO, "{0} reset to FOLLOWER state after election timeout", id);
    }

    /**
     * Method that is periodically called and executes the behavior that is typical for
     * the role the node currently has
     */
    private synchronized void checkState() {
        if (stopped.get()) {
            logger.log(Level.INFO, "{0} is stopped and will not check state", id);
            return;
        }

        long currentTime = System.currentTimeMillis();

        if (state == NodeState.FOLLOWER) {
            if (currentTime >= nextElectionTime) {
                logger.log(Level.INFO, "{0} did not receive heartbeat in time and is starting election", id);
                startElection();
            }
        } else if (state == NodeState.CANDIDATE) {
            int nonStoppedNodes = (int) cluster.getNodes().stream().filter(node -> !node.stopped.get()).count();
            int majority = nonStoppedNodes / 2 + 1;

            if (votesReceived.get() >= majority) {
                becomeLeader();
            } else if (currentTime >= nextElectionTime) {
                logger.log(Level.INFO, "{0} election timeout reached and is starting a new election", id);
                startElection();
            } else if (currentTime - electionStartTime > maxElectionDuration) {
                logger.log(Level.INFO, "{0} election duration exceeded max limit. Resetting to follower state.", id);
                resetToFollower();
            }
        }
    }
}
