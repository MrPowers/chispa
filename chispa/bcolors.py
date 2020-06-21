class bcolors:
    NC='\033[0m' # No Color, reset all

    Bold='\033[1m'
    Underlined='\033[4m'
    Blink='\033[5m'
    Inverted='\033[7m'
    Hidden='\033[8m'

    Black='\033[30m'
    Red='\033[31m'
    Green='\033[32m'
    Yellow='\033[33m'
    Blue='\033[34m'
    Purple='\033[35m'
    Cyan='\033[36m'
    LightGray='\033[37m'
    DarkGray='\033[30m'
    LightRed='\033[31m'
    LightGreen='\033[32m'
    LightYellow='\033[93m'
    LightBlue='\033[34m'
    LightPurple='\033[35m'
    LightCyan='\033[36m'
    White='\033[97m'


def blue(s: str) -> str:
  return bcolors.LightBlue + str(s) + bcolors.LightRed

