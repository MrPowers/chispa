def format_string(input, formats):
    formatting = {
        "nc": '\033[0m',  # No Color, reset all
        "bold": '\033[1m',
        "underline": '\033[4m',
        "blink": '\033[5m',
        "inverted": '\033[7m',
        "hidden": '\033[8m',
        "blue": '\033[34m',
        "white": '\033[97m',
        "red": '\033[31m',
    }
    formatted = input
    for format in formats:
        s = formatting[format]
        formatted = s + str(formatted) + s
    return formatting["nc"] + str(formatted) + formatting["nc"]

def format_mismatched_cell(input_text: str, mismatched_cells) -> str:
    return format_string(input_text, mismatched_cells)