r"""
Build SQLite database from CSV file

AUTHOR:
    Hugo PETETIN

"""

import sqlite3
import pandas as pd


def last_known_eruption_to_iso_8601(lke_string: str) -> str:
    """
    Convert string data found in the DB to ISO-8601 compliant string YYYY-MM-DD
    """

    # Dates may be
    # "Unknown"
    # "{int} CE"
    # "{int} BCE"
    lke_year_diff = 0
    if lke_string == "Unknown":
        return None
    elif "BCE" in lke_string:
        # BCE
        lke_year_diff = -int(lke_string[:-4])
    else:
        # CE
        lke_year_diff = int(lke_string[:-3])
    return lke_year_diff


def main():
    # retrieve the data from the CSV file
    volcano_data = (
        pd.read_csv("data/holocene_volcanoes.csv", sep=";")
        .to_dict(orient="index")
        .values()
    )

    # manually converting the data (to_sql method is too raw)
    with sqlite3.connect("data/holocene_volcanoes.db") as conn:
        cur = conn.cursor()

        # remove the table if it already exists
        cur.execute("DROP TABLE IF EXISTS Volcanoes")
        conn.commit()

        # (re)initialize table
        cur.execute("""
            CREATE TABLE Volcanoes (
                id INTEGER PRIMARY KEY,
                name TEXT,
                country TEXT,
                region_group TEXT,
                region TEXT,
                landform TEXT,
                primary_type TEXT,
                activity_evidence TEXT,
                time_since_last_eruption INTEGER,
                latitude REAL,
                longitude REAL,
                elevation INTEGER,
                tectonic_setting TEXT,
                dominant_rock_type TEXT
            );
        """)
        conn.commit()

        for entry in volcano_data:
            cur.execute(
                """
                INSERT INTO Volcanoes (
                id,
                name,
                country,
                region_group,
                region,
                landform,
                primary_type,
                activity_evidence,
                time_since_last_eruption,
                latitude,
                longitude,
                elevation,
                tectonic_setting,
                dominant_rock_type
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, strftime('%Y', 'now') - ?, ?, ?, ?, ?, ?)
            """,
                (
                    entry["Volcano Number"],
                    entry["Volcano Name"],
                    entry["Country"],
                    entry["Volcanic Region Group"],
                    entry["Volcanic Region"],
                    entry["Volcano Landform"],
                    entry["Primary Volcano Type"],
                    entry["Activity Evidence"],
                    last_known_eruption_to_iso_8601(entry["Last Known Eruption"]),
                    entry["Latitude"].replace(",", "."),
                    entry["Longitude"].replace(",", "."),
                    entry["Elevation (m)"],
                    entry["Tectonic Setting"],
                    entry["Dominant Rock Type"],
                ),
            )
            conn.commit()


if __name__ == "__main__":
    main()
