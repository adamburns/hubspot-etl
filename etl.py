from extract import extract
from load import load
from transform import clean_data


def main():
    load(clean_data(extract()))


main()
