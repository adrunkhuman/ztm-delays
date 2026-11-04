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
        "from_label": "From stop, address or map point",
        "to_label": "To stop, address or map point",
        "from_placeholder": "From: stop or street + number",
        "to_placeholder": "To: stop or street + number",
        "swap": "Swap places",
        "locate": "Use my location",
        "my_location": "My location",
        "locate_error": "Your location is unavailable. Allow location access or type an address.",
        "address": "address",
        "address_error": "Address lookup is unavailable. Try again or pick a point on the map.",
        "pick_map": "Pick a point on the map",
        "warsaw": "Warsaw",
        "near": "Near {name}",
        "map_error": "Map unavailable. Try again or type an address.",
        "date": "Date",
        "time": "Leaving after",
        "today": "Today",
        "tomorrow": "Tomorrow",
        "weekdays": ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"),
        "months": ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"),
        "go": "Search",
        "results": "Connections",
        "earlier": "Earlier routes",
        "later": "Later routes",
        "leave_at": "Leave at",
        "be_at_stop": "At the stop",
        "from_stop": "from",
        "be_there_by": "be there by",
        "direct": "direct",
        "changes": ("change", "changes"),
        "arrives": "arrives",
        "running_late": "usually by",
        "running_late_title": "Arrival if the vehicles run late",
        "wait": "wait",
        "be_here_by": "be here by",
        "walk": "walk",
        "walk_over": "walk {n} m",
        "stops": ("stop", "stops"),
        "loading": "Loading stops…",
        "no_route": "No connection from {origin} to {destination} after {time} this day.",
        "choose": "Choose both places: a stop, a street address or a map point.",
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
        "map_label": "Map of this connection, with where the next vehicle is now",
    },
    "pl": {
        "from": "Skąd",
        "to": "Dokąd",
        "from_label": "Początek: przystanek, adres lub punkt na mapie",
        "to_label": "Cel: przystanek, adres lub punkt na mapie",
        "from_placeholder": "Skąd: przystanek lub ulica i numer",
        "to_placeholder": "Dokąd: przystanek lub ulica i numer",
        "swap": "Zamień miejsca",
        "locate": "Użyj mojej lokalizacji",
        "my_location": "Moja lokalizacja",
        "locate_error": "Nie udało się ustalić lokalizacji. Zezwól na dostęp do niej lub wpisz adres.",
        "address": "adres",
        "address_error": "Wyszukiwanie adresów jest niedostępne. Spróbuj ponownie lub wskaż punkt na mapie.",
        "pick_map": "Wskaż punkt na mapie",
        "warsaw": "Warszawa",
        "near": "W pobliżu {name}",
        "map_error": "Mapa niedostępna. Spróbuj ponownie lub wpisz adres.",
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
        "be_at_stop": "Na przystanku",
        "from_stop": "z przystanku",
        "be_there_by": "bądź o",
        "direct": "bez przesiadek",
        "changes": ("przesiadka", "przesiadki", "przesiadek"),
        "arrives": "przyjazd",
        "running_late": "zwykle do",
        "running_late_title": "Przyjazd, jeśli pojazdy się spóźnią",
        "wait": "czekaj",
        "be_here_by": "bądź tu o",
        "walk": "pieszo",
        "walk_over": "przejdź {n} m",
        "stops": ("przystanek", "przystanki", "przystanków"),
        "loading": "Wczytywanie przystanków…",
        "no_route": "Brak połączenia {origin} → {destination} po {time} tego dnia.",
        "choose": "Wybierz oba miejsca: przystanek, adres lub punkt na mapie.",
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
        "map_label": "Mapa połączenia i miejsce, w którym jest teraz najbliższy pojazd",
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


HOME_CITY = ", Warszawa"


def point_label(name: str, lang: str) -> str:
    """A searched point's label in lang: the map lookup's "near" wording follows the page, and Warsaw goes unsaid.

    The label travels in the URL, so a language switch would otherwise keep the old wording.
    """
    name = name.removesuffix(HOME_CITY)
    for text in TEXT.values():
        prefix = str(text["near"]).removesuffix("{name}")
        if name.startswith(prefix):
            return str(TEXT[lang]["near"]).format(name=name.removeprefix(prefix))
    return name


def day_label(day: date, today: date, lang: str) -> str:
    """Date option text, day before month: 'Today, Sun 4 Oct' or 'Dziś, niedz. 4.10'."""
    text = TEXT[lang]
    weekday = text["weekdays"][day.weekday()]  # type: ignore[index]
    months = text["months"]
    label = f"{weekday} {day.day} {months[day.month - 1]}" if months else f"{weekday} {day.day}.{day.month:02d}"  # type: ignore[index]
    relative = {0: text["today"], 1: text["tomorrow"]}.get((day - today).days)
    return f"{relative}, {label}" if relative else label
