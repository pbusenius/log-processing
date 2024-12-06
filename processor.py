import argparse


parser = argparse.ArgumentParser(
    "Log-Processing", description="Processing of collected log-files"
)


parser.add_argument('-f', '--file')  


def main():
    args = parser.parse_args()
    print(args.file)


if __name__ == "__main__":
    main()
