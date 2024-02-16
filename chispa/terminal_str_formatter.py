def format_string(input, formats):
    formatting = {
        "nc": '\033[0m',  # No Color, reset all
        "bold": '\033[1m',
        "underline": '\033[4m',
        "blink": '\033[5m',
        "blue": '\033[34m',
        "white": '\033[97m',
        "red": '\033[31m',
        "invert": '\033[7m',
        "hide": '\033[8m',
        "black": '\033[30m',
        "green": '\033[32m',
        "yellow": '\033[33m',
        "purple": '\033[35m',
        "cyan": '\033[36m',
        "light_gray": '\033[37m',
        "dark_gray": '\033[30m',
        "light_red": '\033[31m',
        "light_green": '\033[32m',
        "light_yellow": '\033[93m',
        "light_blue": '\033[34m',
        "light_purple": '\033[35m',
        "light_cyan": '\033[36m',
    }
    formatted = input
    for format in formats:
        s = formatting[format]
        formatted = s + str(formatted) + s
    return formatting["nc"] + str(formatted) + formatting["nc"]