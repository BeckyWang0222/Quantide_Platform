# generated by datamodel-codegen:
#   filename:  config.yaml
#   timestamp: 2025-05-22T06:13:52+00:00

from __future__ import annotations

from typing import List

from pydantic import BaseModel


class Redis(BaseModel):
    host: str
    port: int
    password: str
    db: int
    decode_responses: bool
    queue: str


class Clickhouse(BaseModel):
    host: str
    port: int
    database: str
    table: str
    user: str
    password: str


class Tushare(BaseModel):
    token: str


class Default(BaseModel):
    type: str


class JobStores(BaseModel):
    default: Default


class Default1(BaseModel):
    type: str
    max_workers: int


class Executors(BaseModel):
    default: Default1


class JobDefaults(BaseModel):
    coalesce: bool
    max_instances: int


class Scheduler(BaseModel):
    daily_data_time: str
    historical_data_time: str
    stock_list_update_time: str
    trade_cal_update_time: str
    redis_check_time: str
    clickhouse_check_time: str
    max_retries: int
    retry_interval: int
    job_stores: JobStores
    executors: Executors
    job_defaults: JobDefaults


class Logging(BaseModel):
    level: str
    format: str
    file: str
    max_bytes: int
    backup_count: int


class Email(BaseModel):
    enabled: bool
    smtp_server: str
    smtp_port: int
    sender: str
    password: str
    recipients: List[str]


class Monitor(BaseModel):
    enabled: bool
    email: Email


class Model(BaseModel):
    redis: Redis
    clickhouse: Clickhouse
    tushare: Tushare
    scheduler: Scheduler
    logging: Logging
    monitor: Monitor
