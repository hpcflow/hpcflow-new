import sys

if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
    import multiprocessing

    multiprocessing.freeze_support()

from hpcflow.api import hpcflow

if __name__ == "__main__":
    hpcflow.CLI()
