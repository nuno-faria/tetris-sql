#!/usr/bin/python3

import sys
import psycopg2
import argparse


# cross platform get key code
if sys.platform == "win32":
    import msvcrt

    def get_key():
        key = msvcrt.getch()
        if key in [b"\x00", b"\xe0"]:
            key = {b"H": b"up", b"P": b"down", b"K": b"left", b"M": b"right"}.get(msvcrt.getch(), b'')

        return key.decode(errors="ignore")
else:
    import tty
    import termios

    def get_key():
        fd = sys.stdin.fileno()
        orig_settings = termios.tcgetattr(fd)
        tty.setraw(fd)

        key = sys.stdin.read(1)
        if key == "\x1b":
            key += sys.stdin.read(2)
            key = {"\x1b[A": "up","\x1b[B": "down","\x1b[C": "right","\x1b[D": "left"}.get(key, key)

        termios.tcsetattr(fd, termios.TCSADRAIN, orig_settings)
        return key


# arguments
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-H", "--host", type=str, help="Database host", default="localhost")
parser.add_argument("-P", "--port", type=int, help="Database port", default=5432)
parser.add_argument("-d", "--database", type=str, help="Database name", default="postgres")
parser.add_argument("-u", "--user", type=str, help="Database user", default="postgres")
parser.add_argument("-p", "--password", type=str, help="Database password", default="postgres")
args = parser.parse_args()

# connect to db
try:
    print(f"Connecting to {args.host}:{args.port}/{args.database} ... ", end="")
    conn = psycopg2.connect(f"host={args.host} port={args.port} dbname={args.database} " +
                            f"user={args.user} password={args.password}")
    print("connected.")
except Exception as e:
    print(e)
    exit(1)

conn.autocommit = True
cursor = conn.cursor()
keyTranslator = {"w": "up", "a": "left", "s": "down", "d": "right", " ": "space"}

print("Controls:")
print("  Arrow keys/WASD - move")
print("  Space - hard drop")
print("  P - pause (move/hard drop to unpause)")
print("  Q - stop the input script")

# continuously get the keys pressed and update the input table
while True:
    key = get_key().lower()
    key = keyTranslator.get(key, key)

    if key in ("up", "down", "left", "right", "space", "p"):
        try:
            cursor.execute("UPDATE Input SET cmd = %s, ts = clock_timestamp()", (key[0],))
        except psycopg2.errors.UndefinedTable:
            print("Warning: table 'Input' does not exist")
        except psycopg2.errors.OperationalError as e:
            print(f"Connection failed: {e}")
            exit(1)
    elif key == "q":
        break

conn.close()
