"""Count nominal slots without completion-time drift or disabled-time inflation."""


def skipped_after(nominal, completed, interval, *, retired_at=None):
    q,end,step=round(nominal*1000),round(completed*1000),round(interval*1000)
    if retired_at is not None:
        # Configuration retirement excludes the slot exactly at its boundary.
        end=min(end,round(retired_at*1000))
        return max(0,(end-q+step-1)//step-1)
    return max(0,(end-q)//step)


def skipped_before(nominal, retired_at, interval):
    q,end,step=round(nominal*1000),round(retired_at*1000),round(interval*1000)
    return max(0,(end-q+step-1)//step)
