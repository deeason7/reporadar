"""RepoRadar — ecosystem intelligence for open source.

Ingests the public GitHub event stream: a live poller reporting exact counts of
what it pulled, and the hourly GH Archive record kept as deep history. Stores it
honestly, and builds risk, trend, and causal analytics on top.

What share of the firehose the live poller sees is **not measured**. An estimator
derived from the feed's id continuity stood here and was retired: its residual
error had no explanation, and an unexplained factor is not a weaker version of a
right answer.
"""

__version__ = "1.0.0"
