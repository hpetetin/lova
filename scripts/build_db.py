r"""
Build SQLite database from

AUTHOR:
    Hugo PETETIN

"""

import sqlite3
import pandas as pd


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
                last_known_eruption TEXT,
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
                last_known_eruption,
                latitude,
                longitude,
                elevation,
                tectonic_setting,
                dominant_rock_type
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    entry["Last Known Eruption"],
                    entry["Latitude"],
                    entry["Longitude"],
                    entry["Elevation (m)"],
                    entry["Tectonic Setting"],
                    entry["Dominant Rock Type"],
                ),
            )
            conn.commit()


if __name__ == "__main__":
    main()
