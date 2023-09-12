class bcolors:
    nc = '\033[0m'  # No Color, reset all

    # Style
    bold = '\033[1m'
    underlined = '\033[4m'
    blink = '\033[5m'
    inverted = '\033[7m'
    hidden = '\033[8m'

    # Colors
    black = '\033[30m'
    red = '\033[31m'
    green = '\033[32m'
    yellow = '\033[33m'
    blue = '\033[34m'
    purple = '\033[35m'
    cyan = '\033[36m'
    light_gray = '\033[37m'
    dark_gray = '\033[30m'
    light_red = '\033[31m'
    light_green = '\033[32m'
    light_yellow = '\033[93m'
    light_blue = '\033[34m'
    light_purple = '\033[35m'
    light_cyan = '\033[36m'
    white = '\033[97m'


def normal_text(input_text: str, color_scheme: dict) -> str:
    return get_color(color_scheme["matched"]) + input_text + get_color(color_scheme["default"])


def underline_text(input_text: str, color_scheme: dict) -> str:
    """
    Takes an input string and returns a white, underlined string (based on PrettyTable formatting)
    """
    return get_color(color_scheme["underlined"]) + bcolors.underlined + input_text + bcolors.nc + get_color(color_scheme["default"])

def get_color(color_string: str) -> str:
    """
    Takes a color string, e.g. "Red" and returns Pretty Tables color code string if it exists in the bcolors class, otherwise raise an Exception
    """
    color_string_cleaned = color_string.lower() # Clean string
    if hasattr(bcolors(), color_string_cleaned):
        return getattr(bcolors(), color_string_cleaned)
    else:
        raise Exception(f"Unable to find color '{color_string}' in bcolors.")


