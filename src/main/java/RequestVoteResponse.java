public class RequestVoteResponse {

    private final int term;
    private final boolean voteGranted;
    private final String voterId;

    public RequestVoteResponse(int term, boolean voteGranted, String voterId) {
        this.term = term;
        this.voteGranted = voteGranted;
        this.voterId = voterId;
    }

    public int getTerm() {
        return term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public String getVoterId() {
        return voterId;
    }
}
