"""Per-runner state transitions; final physical outcomes and detection-time UTC."""
class PointEvents:
    def __init__(self, samples, latest):
        self.samples, self.latest = samples, latest
        self.context = None
        self.origins = {}
        self.last_result = {}
        self.quality = {}
        self.plan = {}
        self.time_epochs = {}

    def activate(self, context, origins, now):
        if context == self.context:
            return
        boot=self.samples.boot_context
        owner=(context.gateway_id,context.config_version)
        if boot.point_sources_owner!=owner:
            boot.point_sources.clear()
            boot.point_sources_owner=owner
        state=boot.point_sources.get(context.device.device_id)
        if state is not None and state['context']==context:
            self.context=context
            self.origins=state['origins'];self.last_result=state['last_result']
            self.quality=state['quality'];self.plan=state['plan']
            self.time_epochs=state['time_epochs']
            self.latest.update(state['latest'])
            state['latest']=self.latest
            return
        self.context = context
        self.origins = dict(origins)
        self.last_result={};self.quality={};self.plan={};self.time_epochs={}
        self.latest.clear()
        boot.point_sources[context.device.device_id]=dict(context=context,origins=self.origins,
            last_result=self.last_result,quality=self.quality,plan=self.plan,latest=self.latest,time_epochs=self.time_epochs)
        for point in context.device.points:
            if point.enabled:
                self._emit(point, None, 'UNKNOWN', now, None, 'QUALITY_STATE')

    def _emit(self, point, value, quality, completed, nominal, kind, publish=True):
        context = self.context
        clock=self.samples.time_provider.read(round(completed*1000))
        pid=point.point_id
        if publish and pid in self.time_epochs and self.time_epochs[pid]!=clock.generation:
            since=clock.generation_started_mono_ms
            since=round(completed*1000) if since is None else since
            q=nominal if nominal is not None else self.plan.get('next_nominal',{}).get(pid,self.origins[pid])
            q_ms=round(q*1000)
            q_ms+=max(0,(since-q_ms+point.poll_interval_ms-1)//point.poll_interval_ms)*point.poll_interval_ms
            self.origins[pid]=q_ms/1000
        if publish:
            self.time_epochs[pid]=clock.generation
        sample = self.samples.capture(context.config_version, context.device.device_id,
            point.point_id, value, quality, completed=completed, nominal=nominal,
            origin=self.origins[point.point_id], generation=context.generation, kind=kind,
            gateway_id=context.gateway_id or self.samples.gateway_id,clock_reading=clock)
        if publish:
            self.quality[point.point_id] = quality
            self.latest[point.point_id] = sample
            self.samples.put(sample)
        else:
            self.samples.version_discarded_events += 1
            self.samples.undecided_lost_events += 1
            self.samples.accounting_complete = False
            self.samples.boot_context.accounting.undecided(sample,'VERSION_DISCARD')
        return sample

    def result(self, context, point, value, quality, nominal, completed, *, publish=True):
        if context != self.context:
            return None
        if publish:
            self.last_result[point.point_id] = completed
        return self._emit(point, value, quality, completed, nominal, 'PHYSICAL_RESULT', publish)

    def expire(self, now):
        if self.context is None:
            return
        for point in self.context.device.points:
            if not point.enabled or self.quality.get(point.point_id) == 'STALE':
                continue
            last = self.last_result.get(point.point_id, self.origins[point.point_id])
            if now - last >= point.stale_after_ms / 1000:
                self._emit(point, None, 'STALE', now, None, 'QUALITY_STATE')

    def next_expiry(self):
        if self.context is None:
            return float('inf')
        return min((self.last_result.get(p.point_id, self.origins[p.point_id]) + p.stale_after_ms / 1000
                    for p in self.context.device.points if p.enabled and self.quality.get(p.point_id) != 'STALE'),
                   default=float('inf'))
