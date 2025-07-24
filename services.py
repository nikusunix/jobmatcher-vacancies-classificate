import re
import csv

from typing import Set

PATTERN = r"^(.*?)(?=/)"


class CSVParser:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.count = 0

    def parse(self) -> Set[str]:
        profession_titles = set()

        with open(self.file_path, newline="", encoding="utf-8-sig") as csvfile:
            reader = csv.DictReader(csvfile)

            # Parse
            for row in reader:
                raw = row['Klassifikation der Berufe, Ausgabe 2010;""']

                match = re.search(PATTERN, raw)

                if match:
                    profession_titles.add(match.group(1))
                    self.count += 1

        return profession_titles
