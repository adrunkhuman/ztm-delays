"""Planner wording in Polish and English; the rest of the site stays English."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import date

LANGS = ("pl", "en")  # best_match order: a browser accepting anything ("*") gets Polish
DEFAULT_LANG = "pl"

TEXT: dict[str, dict[str, object]] = {
    "en": {
        "from": "From",
        "to": "To",
        "from_label": "From stop",
        "to_label": "To stop",
        "swap": "Swap stops",
        "date": "Date",
        "time": "Leaving after",
        "today": "Today",
        "tomorrow": "Tomorrow",
        "weekdays": ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"),
        "months": ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"),
        "go": "Show routes",
        "results": "Connections",
        "earlier": "Earlier routes",
        "later": "Later routes",
        "leave_at": "Leave at",
        "be_at_stop": "Be at the stop",
        "from_stop": "from",
        "direct": "direct",
        "changes": ("change", "changes"),
        "arrives": "arrives",
        "running_late": "running late:",
        "wait": "wait",
        "be_here_by": "be here by",
        "walk": "walk",
        "stops": ("stop", "stops"),
        "loading": "Loading stops…",
        "no_route": "No connection from {origin} to {destination} after {time} this day.",
        "choose": "Choose both stops from the suggestions.",
        "note": "Changes leave room for a late arrival and the walk.",
        "flag_note": "marks times at least 2 min off the timetable.",
        "timetable": "timetable",
        "differs": "differs from timetable",
        "expected": "expected",
        "modes": {"bus": "bus", "tram": "tram", "metro": "metro", "rail": "rail"},
        "posts": {"metro": "metro", "train": "train"},
        "live": "live position",
        "live_late": "{n} min late",
        "live_on_time": "on time",
        "live_early": "{n} min early",
        "live_waiting": "at terminus",
        "live_waiting_late": "at terminus, +{n} min",
        "live_previous": "on previous trip",
        "live_previous_late": "previous trip, +{n} min",
        "map_label": "Where the vehicle is now and your stop",
        "live_note": "Live times follow where the vehicles are now.",
    },
    "pl": {
        "from": "Skąd",
        "to": "Dokąd",
        "from_label": "Przystanek początkowy",
        "to_label": "Przystanek docelowy",
        "swap": "Zamień przystanki",
        "date": "Dzień",
        "time": "Odjazd po",
        "today": "Dziś",
        "tomorrow": "Jutro",
        "weekdays": ("pon.", "wt.", "śr.", "czw.", "pt.", "sob.", "niedz."),
        "months": None,  # day.month numerals, e.g. 4.10
        "go": "Szukaj",
        "results": "Połączenia",
        "earlier": "Wcześniejsze połączenia",
        "later": "Późniejsze połączenia",
        "leave_at": "Wyjdź o",
        "be_at_stop": "Bądź na przystanku",
        "from_stop": "z przystanku",
        "direct": "bez przesiadek",
        "changes": ("przesiadka", "przesiadki", "przesiadek"),
        "arrives": "przyjazd",
        "running_late": "przy opóźnieniu:",
        "wait": "czekaj",
        "be_here_by": "bądź tu o",
        "walk": "pieszo",
        "stops": ("przystanek", "przystanki", "przystanków"),
        "loading": "Wczytywanie przystanków…",
        "no_route": "Brak połączenia {origin} → {destination} po {time} tego dnia.",
        "choose": "Wybierz oba przystanki z podpowiedzi.",
        "note": "Przesiadki mają zapas na spóźnienie pojazdu i dojście.",
        "flag_note": "oznacza czas różny od rozkładu o co najmniej 2 min.",
        "timetable": "rozkład",
        "differs": "inny niż w rozkładzie",
        "expected": "przewidywany",
        "modes": {"bus": "autobus", "tram": "tramwaj", "metro": "metro", "rail": "SKM"},
        "posts": {"metro": "metro", "train": "SKM"},
        "live": "pozycja na żywo",
        "live_late": "spóźniony {n} min",
        "live_on_time": "punktualnie",
        "live_early": "{n} min przed czasem",
        "live_waiting": "na pętli",
        "live_waiting_late": "na pętli, +{n} min",
        "live_previous": "poprzedni kurs",
        "live_previous_late": "poprzedni kurs, +{n} min",
        "map_label": "Gdzie jest teraz pojazd i twój przystanek",
        "live_note": "Czasy na żywo według tego, gdzie są teraz pojazdy.",
    },
}


def plural(count: int, forms: Sequence[str]) -> str:
    """The noun form for a count: English (one, other) or Polish (one, few, many)."""
    if count == 1:
        return forms[0]
    if len(forms) == 2:  # noqa: PLR2004
        return forms[1]
    few = count % 10 in {2, 3, 4} and count % 100 not in {12, 13, 14}
    return forms[1] if few else forms[2]


def day_label(day: date, today: date, lang: str) -> str:
    """Date option text, day before month: 'Today, Sun 4 Oct' or 'Dziś, niedz. 4.10'."""
    text = TEXT[lang]
    weekday = text["weekdays"][day.weekday()]  # type: ignore[index]
    months = text["months"]
    label = f"{weekday} {day.day} {months[day.month - 1]}" if months else f"{weekday} {day.day}.{day.month:02d}"  # type: ignore[index]
    relative = {0: text["today"], 1: text["tomorrow"]}.get((day - today).days)
    return f"{relative}, {label}" if relative else label
