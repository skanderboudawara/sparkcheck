from operator import eq, ge, gt, le, lt, ne
# Map operator strings to corresponding functions
OPERATOR_MAP = {
    "<": lt,
    "<=": le,
    "==": eq,
    "!=": ne,
    ">": gt,
    ">=": ge,
}