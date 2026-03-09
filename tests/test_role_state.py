from src.core.raft.role_state import Role, RoleState


def test_become_leader_resets_vote_and_triggers_callback():
    callback_calls = []
    state = RoleState(role=Role.FOLLOWER, term=3, voted_for=7)
    state._on_become_leader = lambda: callback_calls.append("leader")

    state.become_leader()

    assert state.role == Role.LEADER
    assert state.voted_for is None
    assert callback_calls == ["leader"]


def test_become_follower_updates_term_and_clears_vote_for_newer_term():
    callback_calls = []
    state = RoleState(role=Role.CANDIDATE, term=2, voted_for=4)
    state._on_become_follower = lambda: callback_calls.append("follower")

    state.become_follower(5)

    assert state.role == Role.FOLLOWER
    assert state.term == 5
    assert state.voted_for is None
    assert callback_calls == ["follower"]


def test_become_follower_keeps_term_and_vote_for_same_or_older_term():
    state = RoleState(role=Role.CANDIDATE, term=4, voted_for=2)

    state.become_follower(4)

    assert state.role == Role.FOLLOWER
    assert state.term == 4
    assert state.voted_for == 2


def test_become_candidate_increments_term_sets_vote_and_callback():
    callback_calls = []
    state = RoleState(role=Role.FOLLOWER, term=8)
    state._on_become_candidate = lambda: callback_calls.append("candidate")

    state.become_candidate(11)

    assert state.role == Role.CANDIDATE
    assert state.term == 9
    assert state.voted_for == 11
    assert callback_calls == ["candidate"]
