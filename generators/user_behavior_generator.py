from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import random


@dataclass
class UserBehaviorGenerator:
    seed: int | None = None
    randomizer: random.Random = field(init=False)
    counter: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        self.randomizer = random.Random(self.seed)

    def generate_event(self) -> dict[str, str]:
        self.counter += 1
        return {
            "event_id": f"evt_{self.counter:06d}",
            "user_id": f"u_{self.randomizer.randint(1000, 9999)}",
            "product_id": f"p_{self.randomizer.randint(1000, 9999)}",
            "event_type": self.randomizer.choice(["view", "click", "cart"]),
            "event_time": datetime.now(UTC).isoformat(),
            "channel": self.randomizer.choice(["app", "web", "mini_program"]),
            "device_type": self.randomizer.choice(["ios", "android", "pc"]),
            "page_id": self.randomizer.choice(
                ["home", "search_result", "product_detail", "cart_page"]
            ),
        }
