"""MCP tool annotations for the tools this server registers.

The protocol's defaults are not the cautious ones. A tool that omits these
hints is read by a client as a call that modifies its environment, destroys
data and reaches an open-ended set of entities:

    readOnlyHint    default false
    destructiveHint default true   (only meaningful when readOnlyHint is false)
    idempotentHint  default false  (only meaningful when readOnlyHint is false)
    openWorldHint   default true

So leaving them off is a claim, not a blank. Without them `get`, `hgetall` and
`info` are presented as calls that can destroy a database, and a client that
puts destructive tools behind a confirmation prompt stops the user on every
read.

The presets below are named after the hints they set rather than after a Redis
command group, so that assigning one to a new tool is a question about what the
command does to the data, not about which file it lives in.

`openWorldHint` is false throughout: every tool here talks to the one Redis
instance the server was configured with. The hint is about whether the set of
entities a tool can reach is open-ended, as it is for a web search, not about
whether the call leaves the process.
"""

from mcp.types import ToolAnnotations

# `idempotentHint` is left off: the spec gives it meaning only alongside a
# write, so a read-only tool has nothing to say with it.
READ_ONLY = ToolAnnotations(
    readOnlyHint=True,
    destructiveHint=False,
    openWorldHint=False,
)

# Brings new data into being and leaves what is already stored alone. Running it
# twice is not the same as running it once: `lpush` appends again, `xadd` adds a
# second entry.
WRITE_ADDITIVE = ToolAnnotations(
    readOnlyHint=False,
    destructiveHint=False,
    idempotentHint=False,
    openWorldHint=False,
)

# Additive, and running it again lands on the same state: `sadd` of a member
# that is already in the set, `xack` of an entry already acknowledged.
WRITE_ADDITIVE_IDEMPOTENT = ToolAnnotations(
    readOnlyHint=False,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=False,
)

# Replaces or removes data that is already there, so a previous value is gone.
# Destructive in the protocol's sense, which is about replacing and not only
# about deleting. Repeating it lands on the same state.
WRITE_DESTRUCTIVE = ToolAnnotations(
    readOnlyHint=False,
    destructiveHint=True,
    idempotentHint=True,
    openWorldHint=False,
)

# Destructive, and each call takes something further: `lpop` removes the next
# element, `rename` fails once the source key is gone.
WRITE_DESTRUCTIVE_NOT_IDEMPOTENT = ToolAnnotations(
    readOnlyHint=False,
    destructiveHint=True,
    idempotentHint=False,
    openWorldHint=False,
)
