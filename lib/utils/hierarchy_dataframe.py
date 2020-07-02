#!/usr/bin/env python3
import pandas as pd
from typing import List


def hierarchy_dataframe(metadata: pd.DataFrame, unique_id: str,
    hierarchy_columns: List[str]) -> pd.DataFrame:
    """
    Given a ``pd.DataFrame`` `metadata`, returns a `pd.DataFrame` with one row
    for every unique location hierarchy defined by the `unique_id` and
    `hierarchy_columns` (e.g. region, country, and division). Looks for location
    columns matching the original resolution name (e.g. 'region') and any
    stubname of it (e.g. 'region_exposure').

    Raises a :class:``KeyError`` if the given *unique_id* isn't in the
    *metadata* DataFrame.
    """
    try:
        metadata[unique_id]
    except KeyError:
        raise KeyError(f"The column «{unique_id}» does not exist in the given "
            "metadata.")

    metadata = metadata.rename(columns={
        resolution: f'{resolution}_strain' for resolution in hierarchy_columns
    })

    return pd \
        .wide_to_long(metadata,
            stubnames=hierarchy_columns,
            i=unique_id,
            j="resolution_type",
            sep='_',
            suffix=r'\w+') \
        .reset_index()[hierarchy_columns] \
        .fillna('') \
        .drop_duplicates() \
        .reset_index(drop=True) \
        .sort_values(by=hierarchy_columns)
