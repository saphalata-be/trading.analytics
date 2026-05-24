from __future__ import annotations

from dataclasses import dataclass


ENTRY_FILTER_NONE = 0
ENTRY_FILTER_INITIAL_MOVE = 1

DEFAULT_ENTRY_FILTER_ID = ENTRY_FILTER_NONE
DEFAULT_INITIAL_MOVE_ATR = 2.0
DEFAULT_INITIAL_RETRACE_ATR = 0.5


@dataclass(frozen=True)
class EntryFilterConfig:
    filter_id: int = DEFAULT_ENTRY_FILTER_ID
    initial_move_atr: float | None = None
    initial_retrace_atr: float | None = None


def normalize_entry_filter(
    filter_id: int | None = None,
    initial_move_atr: float | None = None,
    initial_retrace_atr: float | None = None,
) -> EntryFilterConfig:
    normalized_filter_id = DEFAULT_ENTRY_FILTER_ID if filter_id is None else int(filter_id)

    if normalized_filter_id == ENTRY_FILTER_NONE:
        return EntryFilterConfig(filter_id=ENTRY_FILTER_NONE)

    if normalized_filter_id == ENTRY_FILTER_INITIAL_MOVE:
        move_atr = DEFAULT_INITIAL_MOVE_ATR if initial_move_atr is None else float(initial_move_atr)
        retrace_atr = (
            DEFAULT_INITIAL_RETRACE_ATR
            if initial_retrace_atr is None
            else float(initial_retrace_atr)
        )
        if move_atr <= 0:
            raise ValueError("Le mouvement initial doit etre strictement positif.")
        if retrace_atr <= 0:
            raise ValueError("Le retour initial doit etre strictement positif.")
        return EntryFilterConfig(
            filter_id=ENTRY_FILTER_INITIAL_MOVE,
            initial_move_atr=move_atr,
            initial_retrace_atr=retrace_atr,
        )

    raise ValueError(f"Filtre d'entree inconnu: {normalized_filter_id}")


def entry_filter_payload(config: EntryFilterConfig) -> dict:
    return {
        "entry_filter_id": config.filter_id,
        "initial_move_atr": config.initial_move_atr,
        "initial_retrace_atr": config.initial_retrace_atr,
        "entry_filter_label": entry_filter_label(config),
    }


def entry_filter_label(config: EntryFilterConfig) -> str:
    if config.filter_id == ENTRY_FILTER_NONE:
        return "Filtre 0 - Sans filtre"
    if config.filter_id == ENTRY_FILTER_INITIAL_MOVE:
        return (
            "Filtre 1 - Mouvement initial "
            f"{config.initial_move_atr:g} ATR puis retour {config.initial_retrace_atr:g} ATR"
        )
    return f"Filtre {config.filter_id}"


def entry_filter_cache_parts(config: EntryFilterConfig) -> tuple[int, float, float]:
    return (
        config.filter_id,
        0.0 if config.initial_move_atr is None else config.initial_move_atr,
        0.0 if config.initial_retrace_atr is None else config.initial_retrace_atr,
    )


def find_entry_for_arrays(
    bars_open: list,
    bars_high: list,
    bars_low: list,
    start_idx: int,
    atr50: float,
    direction: str,
    config: EntryFilterConfig,
) -> tuple[int, float] | None:
    if config.filter_id == ENTRY_FILTER_NONE:
        return start_idx, bars_open[start_idx]

    if config.filter_id != ENTRY_FILTER_INITIAL_MOVE:
        raise ValueError(f"Filtre d'entree inconnu: {config.filter_id}")

    start_price = bars_open[start_idx]
    move = config.initial_move_atr * atr50
    retrace = config.initial_retrace_atr * atr50

    if direction == "LONG":
        move_price = start_price - move
        lowest = start_price
        move_reached = False
        for i in range(start_idx, len(bars_open)):
            bar_low = bars_low[i]
            bar_high = bars_high[i]
            if bar_low < lowest:
                lowest = bar_low
            if not move_reached and bar_low <= move_price:
                move_reached = True
                lowest = min(lowest, move_price)
            if move_reached:
                entry_price = lowest + retrace
                if bar_high >= entry_price:
                    return i, entry_price
        return None

    if direction == "SHORT":
        move_price = start_price + move
        highest = start_price
        move_reached = False
        for i in range(start_idx, len(bars_open)):
            bar_high = bars_high[i]
            bar_low = bars_low[i]
            if bar_high > highest:
                highest = bar_high
            if not move_reached and bar_high >= move_price:
                move_reached = True
                highest = max(highest, move_price)
            if move_reached:
                entry_price = highest - retrace
                if bar_low <= entry_price:
                    return i, entry_price
        return None

    raise ValueError(f"Direction inconnue: {direction}")


def find_sequential_entries_for_arrays(
    bars_open: list,
    bars_high: list,
    bars_low: list,
    atr50_by_idx: dict[int, float],
    direction: str,
    config: EntryFilterConfig,
) -> list[tuple[int, float]]:
    if config.filter_id == ENTRY_FILTER_NONE:
        return [(idx, bars_open[idx]) for idx in sorted(atr50_by_idx)]

    if config.filter_id != ENTRY_FILTER_INITIAL_MOVE:
        raise ValueError(f"Filtre d'entree inconnu: {config.filter_id}")

    entries: list[tuple[int, float]] = []
    move_reached = False

    if direction == "LONG":
        peak: float | None = None
        lowest_after_move: float | None = None
        retrace_distance: float | None = None

        for i in range(len(bars_open)):
            atr50 = atr50_by_idx.get(i)
            if atr50 is None or atr50 <= 0:
                peak = None
                move_reached = False
                lowest_after_move = None
                retrace_distance = None
                continue

            bar_high = bars_high[i]
            bar_low = bars_low[i]

            if peak is None:
                peak = bars_open[i]

            if not move_reached:
                if bar_high > peak:
                    peak = bar_high
                if bar_low <= peak - config.initial_move_atr * atr50:
                    move_reached = True
                    lowest_after_move = bar_low
                    retrace_distance = config.initial_retrace_atr * atr50
                continue

            if bar_low < lowest_after_move:
                lowest_after_move = bar_low

            entry_price = lowest_after_move + retrace_distance
            if bar_high >= entry_price:
                entries.append((i, entry_price))
                peak = None
                move_reached = False
                lowest_after_move = None
                retrace_distance = None

        return entries

    if direction == "SHORT":
        trough: float | None = None
        highest_after_move: float | None = None
        retrace_distance: float | None = None

        for i in range(len(bars_open)):
            atr50 = atr50_by_idx.get(i)
            if atr50 is None or atr50 <= 0:
                trough = None
                move_reached = False
                highest_after_move = None
                retrace_distance = None
                continue

            bar_high = bars_high[i]
            bar_low = bars_low[i]

            if trough is None:
                trough = bars_open[i]

            if not move_reached:
                if bar_low < trough:
                    trough = bar_low
                if bar_high >= trough + config.initial_move_atr * atr50:
                    move_reached = True
                    highest_after_move = bar_high
                    retrace_distance = config.initial_retrace_atr * atr50
                continue

            if bar_high > highest_after_move:
                highest_after_move = bar_high

            entry_price = highest_after_move - retrace_distance
            if bar_low <= entry_price:
                entries.append((i, entry_price))
                trough = None
                move_reached = False
                highest_after_move = None
                retrace_distance = None

        return entries

    raise ValueError(f"Direction inconnue: {direction}")
