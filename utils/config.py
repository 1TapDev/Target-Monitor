import json
import os
from typing import Dict, List, Any
from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    host: str
    port: int
    database: str
    username: str
    password: str


@dataclass
class MonitorConfig:
    skus: List[str]
    zip_codes: List[str]
    discord_webhook_url: str
    database: DatabaseConfig
    monitoring_interval: int
    send_initial_stock_report: bool = True
    discord_bot_token: str = ""


class ConfigLoader:
    def __init__(self, config_path: str = "config.json"):
        self.config_path = config_path

    def load_config(self) -> MonitorConfig:
        """Load configuration from JSON file"""
        if not os.path.exists(self.config_path):
            self._create_default_config()

        with open(self.config_path, 'r') as f:
            config_data = json.load(f)

        return MonitorConfig(
            skus=config_data.get("skus", []),
            zip_codes=config_data.get("zip_codes", []),
            discord_webhook_url=config_data.get("discord_webhook_url", ""),
            database=DatabaseConfig(**config_data.get("database", {})),
            monitoring_interval=config_data.get("monitoring_interval", 120),
            send_initial_stock_report=config_data.get("send_initial_stock_report", True),
            discord_bot_token=config_data.get("discord_bot_token", "")
        )

    def _create_default_config(self):
        """Create a default config.json file"""
        default_config = {
            "skus": ["6624827"],
            "zip_codes": ["30313"],
            "discord_webhook_url": "https://discord.com/api/webhooks/YOUR_WEBHOOK_URL",
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "target_monitor",
                "username": "postgres",
                "password": "password"
            },
            "monitoring_interval": 120,
            "send_initial_stock_report": True,
            "discord_bot_token": ""
        }

        with open(self.config_path, 'w') as f:
            json.dump(default_config, f, indent=4)

        print(f"Created default config file at {self.config_path}")
        print("Please update the configuration with your actual values.")