"""Fencing test doubles retained in the tracked test tree."""

from shared.generated.common_pb2 import MutationContext
from services.triage.validation import FencingValidator


class MockFencingValidator(FencingValidator):
    """Single-process validator for unit tests; not distributed safety proof."""

    def __init__(self, accept_all: bool = True):
        self.accept_all = accept_all
        self.last_fencing_token: dict[str, int] = {}

    def validate(self, context: MutationContext) -> bool:
        if not self.accept_all or not context.lock_id or not context.owner_id:
            return False
        current_max = self.last_fencing_token.get(context.pool_id, 0)
        if context.fencing_token < current_max:
            return False
        self.last_fencing_token[context.pool_id] = context.fencing_token
        return True
