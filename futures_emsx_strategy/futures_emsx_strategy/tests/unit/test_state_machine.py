from futures_emsx_strategy.core.enums import OrderStatus
from futures_emsx_strategy.execution.emsx_state_machine import EMSXStateMachine


def test_legal_transition():
    sm = EMSXStateMachine()
    assert sm.transition("X", OrderStatus.SENT) is False  # NEW -> SENT not allowed
    assert sm.transition("X", OrderStatus.RISK_APPROVED) is True
    assert sm.transition("X", OrderStatus.SENT) is True
    assert sm.transition("X", OrderStatus.WORKING) is True
    assert sm.transition("X", OrderStatus.FILLED) is True


def test_post_terminal_is_violation():
    sm = EMSXStateMachine()
    violations = []
    sm.on_violation(lambda oid, cur, new: violations.append((oid, cur, new)))
    sm.transition("X", OrderStatus.RISK_APPROVED)
    sm.transition("X", OrderStatus.SENT)
    sm.transition("X", OrderStatus.FILLED)
    sm.transition("X", OrderStatus.WORKING)
    assert violations
