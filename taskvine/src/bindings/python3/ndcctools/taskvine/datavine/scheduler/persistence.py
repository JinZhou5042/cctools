"""Persistence retry policy."""

import dataclasses


@dataclasses.dataclass(frozen=True)
class PersistencePolicy:
    injected_failures: int
    maximum_retries: int
    retry_base_seconds: float
    retry_max_seconds: float
    failure_delay_seconds: float

    @classmethod
    def from_options(
        cls,
        injected_failures,
        maximum_retries,
        retry_base_seconds,
        retry_max_seconds,
        failure_delay_seconds,
    ):
        policy = cls(
            int(injected_failures),
            int(maximum_retries),
            float(retry_base_seconds),
            float(retry_max_seconds),
            float(failure_delay_seconds),
        )
        if (
            policy.injected_failures < 0
            or policy.maximum_retries < 0
            or policy.retry_base_seconds < 0
            or policy.retry_max_seconds < 0
            or policy.failure_delay_seconds < 0
        ):
            raise ValueError(
                "external persistence failure/retry values cannot be negative"
            )
        return policy

    def retry_delay(self, retries):
        retries = int(retries)
        if retries < 0:
            raise ValueError("retry count cannot be negative")
        return min(
            self.retry_base_seconds * (2 ** min(retries, 30)),
            self.retry_max_seconds,
        )
