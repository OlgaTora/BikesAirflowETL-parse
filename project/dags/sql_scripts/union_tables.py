def union_equal_tables(list_tables) -> str:
    tables = [f"Select * from {x}" for x in list_tables]
    return ("""

    union

    """.join(tables))
