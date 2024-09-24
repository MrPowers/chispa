from __future__ import annotations

import warnings


class bcolors:
    NC = "\033[0m"  # No Color, reset all

    Bold = "\033[1m"
    Underlined = "\033[4m"
    Blink = "\033[5m"
    Inverted = "\033[7m"
    Hidden = "\033[8m"

    Black = "\033[30m"
    Red = "\033[31m"
    Green = "\033[32m"
    Yellow = "\033[33m"
    Blue = "\033[34m"
    Purple = "\033[35m"
    Cyan = "\033[36m"
    LightGray = "\033[37m"
    DarkGray = "\033[30m"
    LightRed = "\033[31m"
    LightGreen = "\033[32m"
    LightYellow = "\033[93m"
    LightBlue = "\033[34m"
    LightPurple = "\033[35m"
    LightCyan = "\033[36m"
    White = "\033[97m"

    # Style
    Bold = "\033[1m"
    Underline = "\033[4m"

    def __init__(self) -> None:
        warnings.warn("The `bcolors` class is deprecated and will be removed in a future version.", DeprecationWarning)


def blue(s: str) -> str:
    warnings.warn("The `blue` function is deprecated and will be removed in a future version.", DeprecationWarning)
    return bcolors.LightBlue + str(s) + bcolors.LightRed


def line_blue(s: str) -> str:
    return bcolors.LightBlue + s + bcolors.NC


def line_red(s: str) -> str:
    return bcolors.LightRed + s + bcolors.NC


def underline_text(input_text: str) -> str:
    """
    Takes an input string and returns a white, underlined string (based on PrettyTable formatting)
    """
    warnings.warn(
        "The `underline_text` function is deprecated and will be removed in a future version.", DeprecationWarning
    )
    return bcolors.White + bcolors.Underline + input_text + bcolors.NC + bcolors.LightRed
