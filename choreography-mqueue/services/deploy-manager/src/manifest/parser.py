# src/manifest/parser.py
from typing import List
import yaml
from .exceptions import ManifestNotFoundError, ParserError
from ..domain.task import Task
from ..domain.schedule import Schedule


class ManifestParser:
    # Only fifo and rr for now
    VALID_POLICIES = {"fifo", "rr", "deadline", "other"}

    def __init__(self, manifest_path: str):
        self.manifest_path = manifest_path

    def parse(self) -> Schedule:
        """
        Parse a YAML manifest file and return a Schedule object composed of Task objects.
        """
        try:
            with open(self.manifest_path, "r") as f:
                data = yaml.safe_load(f)
        except FileNotFoundError:
            raise ManifestNotFoundError(f"Manifest file not found: {self.manifest_path}")
        except yaml.YAMLError as e:
            raise ParserError(f"Error parsing YAML manifest: {e}")

        # Validate top-level fields
        if "schedule" not in data:
            raise ParserError("Manifest missing required field: 'schedule'")

        schedule_data = data["schedule"]
        tasks_data = schedule_data.get("tasks", [])

        # Parse tasks into domain Task objects
        tasks: List[Task] = []
        for t in tasks_data:
            try:
                task = Task(
                    id=int(t["id"]),
                    image=t["image"],
                    start=int(t["start"]),
                    deadline=int(t["deadline"]),
                    cpu_affinity=int(t["cpu_affinity"]),
                    policy=t["policy"].lower(),
                    priority=int(t["priority"]),
                    inputs=[{k: v["value"] for k, v in t.get("inputs", {}).items()}],
                    outputs= {k: v["type"] for k, v in t.get("outputs", {}).items()} #t.get("outputs",{})
                )
            except KeyError as e:
                raise ParserError(f"Task missing required field: {e}")

            # Validate policy
            if task.policy not in self.VALID_POLICIES:
                raise ParserError(f"Invalid policy '{task.policy}' in task {task.id}")

            tasks.append(task)

        # Build Schedule domain object
        schedule = Schedule(
            name=schedule_data.get("name", "unnamed"),
            version=schedule_data.get("version", "0.0.0"),
            description=schedule_data.get("description", ""),
            iterations=int(schedule_data.get("iterations", "-1")),
            tasks=tasks
        )


        return schedule
