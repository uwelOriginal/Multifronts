from __future__ import annotations
from core.context import AppContext, FilterState

class BaseView:
    def __init__(self, ctx: AppContext, filters: FilterState):
        self.ctx = ctx
        self.f = filters

    def render(self):
        raise NotImplementedError
