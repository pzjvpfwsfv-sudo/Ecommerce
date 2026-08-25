from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
import random
from uuid import UUID, uuid4


EventIdFactory = Callable[[], UUID]


@dataclass
class UserBehaviorGenerator:
    seed: int | None = None
    event_id_factory: EventIdFactory = uuid4
    randomizer: random.Random = field(init=False)

    def __post_init__(self) -> None:
        self.randomizer = random.Random(self.seed)

    def generate_event(self) -> dict[str, str]:
        event_id = self.event_id_factory()
        if not isinstance(event_id, UUID):
            raise TypeError("event_id_factory must return UUID")
        return {
            "event_id": f"evt_{event_id.hex}",
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
