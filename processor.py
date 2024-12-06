import argparse
import zipfile

from src.visualization import graph

parser = argparse.ArgumentParser(
    "Log-Processing", description="Processing of collected log-files"
)


parser.add_argument("-f", "--file")


def open_zip(file: str, target: str):
    with zipfile.ZipFile(file, "r") as zip_ref:
        zip_ref.extractall(target)


def main():
    args = parser.parse_args()
    print(args.file)

    graph.plot_network()


if __name__ == "__main__":
    main()
