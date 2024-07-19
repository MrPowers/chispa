from __future__ import annotations

from chispa.formatting.formats import RESET, Format


def format_string(input_string: str, format: Format) -> str:
    if not format.color and not format.style:
        return input_string

    formatted_string = input_string
    codes = []

    if format.style:
        for style in format.style:
            codes.append(style)

    if format.color:
        codes.append(format.color)

    formatted_string = "".join(codes) + formatted_string + RESET
    return formatted_string
