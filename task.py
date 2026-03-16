# -*- coding: utf-8 -*-
from .task_connection import TaskConnection
from .task_sync import TaskSync
from .task_group import TaskGroup
from .task_sheet import TaskSheet
from .task_profile import TaskProfile
from .task_m3u import TaskM3U


class Task(TaskConnection, TaskSync, TaskGroup, TaskSheet, TaskProfile, TaskM3U):
    pass
